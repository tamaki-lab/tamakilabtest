from io import BytesIO
from pathlib import Path
from tqdm import tqdm
import os
import av
import random
import json
from multiprocessing import Process, Manager
import queue
import pandas as pd

from utils import bytes2kmg, short_side, MyManager, MyShardWriter
from args import arg_factory


def worker(df, subset, q, lock, pbar, sink, quality, pos):

    # with open('/mnt/NAS-TVS872XT/dataset/ActivityNet/json/activity_net.v1-3.min.json.formatted', 'r') as f:
    #     json_file = json.load(f)
    # df = pd.DataFrame(json_file['database']).transpose()
    while True:
        try:
            video_file_path = q.get(timeout=1)
        except queue.Empty:
            return
        if video_file_path is None:
            return

        #
        # open a video file
        #

        try:
            container = av.open(str(video_file_path))
        except Exception as e:
            print(e)
            continue
        if len(container.streams.video) == 0:
            print(f'{video_file_path.name} have no video streams. skip.')
            continue

        video_stream_id = 0  # default
        stream = container.streams.video[video_stream_id]

        if stream.frames > 0:
            n_frames = stream.frames
        else:
            # stream.frames is not available for some codecs
            n_frames = int(float(container.duration)
                           / av.time_base * stream.base_rate)

        #
        # split frames
        #
        jpg_byte_video_list = []
        frame_sec_video_list = []
        video_stats_dict_list = []
        try:
            for i in range(len(df.loc[video_file_path.name.split('.')[0]]['timestamps'])):
                caption = df.loc[video_file_path.name.split(
                    '.')[0]]['sentences'][i]
                start_time, end_time = df.loc[video_file_path.name.split(
                    '.')[0]]['timestamps'][i]
                start_time = float(start_time)
                end_time = float(end_time)
                video_action_id = video_file_path.stem + '_' + str(i)
                jpg_byte_list = []
                frame_sec_list = []
                resize_w, resize_h = short_side(
                    w=stream.codec_context.width,
                    h=stream.codec_context.height,
                    size=args.short_side_size)

                with tqdm(
                    container.decode(stream),
                    total=n_frames,
                    position=pos + 1,
                    leave=False,
                    mininterval=0.5,
                ) as frame_pbar:
                    frame_pbar.set_description(f"worker {pos:02d}")
                    for frame in frame_pbar:
                        if frame.time < start_time:
                            continue
                        if frame.time > end_time:
                            break
                        frame_sec_list.append(frame.time)
                        img = frame.to_image(width=resize_w,
                                             height=resize_h)
                        with BytesIO() as buffer:
                            img.save(buffer,
                                     format='JPEG',
                                     quality=quality)
                            jpg_byte_list.append(buffer.getvalue())
                if (len(frame_sec_list)) < 16:
                    continue
                #
                # prepare
                #

                key_str = video_file_path.stem

                video_stats_dict = {
                    '__key__': key_str + '_' + str(i),
                    'video_id': video_file_path.stem,
                    'video_action_id': video_action_id,
                    'filename': video_file_path.name,
                    'caption': caption,
                    'width': stream.codec_context.width,
                    'height': stream.codec_context.height,
                    'fps': float(stream.base_rate),
                    'n_frames': n_frames,
                    'duraion': float(container.duration) / av.time_base,
                    'timestamps': frame_sec_list,
                }
                jpg_byte_video_list.append(jpg_byte_list)
                frame_sec_video_list.append(frame_sec_list)
                video_stats_dict_list.append(video_stats_dict)
            #
            # write
            #
        except Exception as e:
            print(e)
            with lock:
                pbar.update(1)
        else:
            with lock:
                for i in range(len(video_stats_dict_list)):
                    video_stats_dict['shard'] = sink.get_shards()

                    sample_dic = {
                        '__key__': key_str + '_' + str(i),
                        'video.pickle': (jpg_byte_video_list[i], frame_sec_video_list[i]),
                        'stats.json': json.dumps(video_stats_dict_list[i]),
                    }

                    sink.write(sample_dic)
                pbar.update(1)
                pbar.set_postfix_str(
                    f"shard {sink.get_shards()}, "
                    f"size {bytes2kmg(sink.get_size())}")


def make_shards(args):

    video_file_paths = [
        path for path in Path(args.dataset_path).glob('*')
        if not path.is_dir()
    ]
    if args.shuffle:
        random.shuffle(video_file_paths)
    n_samples = len(video_file_paths)

    # https://github.com/pytorch/vision/blob/a8bde78130fd8c956780d85693d0f51912013732/torchvision/datasets/folder.py#L36

    with open(args.json_path, 'r') as f:
        json_file = json.load(f)
    df = pd.DataFrame(json_file).transpose()
    # df = df[df['subset'] != 'testing']
    # df = df[df['subset'] != 'validation']
    # df = df[df['subset'] != 'training']
    video_train_file_paths = []
    video_val_file_paths = []
    for i in range(len(video_file_paths)):
        try:
            # print(df.index.get_loc(video_file_paths[i].name[2:].split('.')[0]))
            video_train_file_paths.append(video_file_paths[i])
        except Exception as e:
            video_val_file_paths.append(video_file_paths[i])
    # exit(0)

    shard_dir_path = Path(args.shard_path)
    shard_dir_path.mkdir(exist_ok=True)
    shard_filename = str(shard_dir_path / f'{args.shard_prefix}-%05d.tar')

    # https://qiita.com/tttamaki/items/96b65e6555f9d255ffd9
    MyManager.register('Tqdm', tqdm)
    MyManager.register('Sink', MyShardWriter)

    with MyManager() as my_manager, \
            Manager() as manager:

        #
        # prepare manager objects
        #
        q = manager.Queue()
        lock = manager.Lock()
        pbar = my_manager.Tqdm(
            total=n_samples,
            position=0,
        )
        pbar.set_description("Main process")
        sink = my_manager.Sink(
            pattern=shard_filename,
            maxsize=int(args.max_size_gb * 1000**3),
            maxcount=args.max_count)

        #
        # start workers
        #
        p_all = [Process(target=worker,
                         args=(df, args.subset, q, lock, pbar, sink,
                               args.quality, i))
                 for i in range(args.num_workers)]
        [p.start() for p in p_all]

        for item in video_train_file_paths:
            q.put(item)
        for _ in range(args.num_workers):
            q.put(None)

        #
        # wait workers, then close
        #
        [p.join() for p in p_all]
        [p.close() for p in p_all]

        dataset_size_filename = str(
            shard_dir_path / f'{args.shard_prefix}-dataset-size.json')
        with open(dataset_size_filename, 'w') as fp:
            json.dump({
                "dataset size": sink.get_counter(),
            }, fp)

        sink.close()
        pbar.close()


if __name__ == '__main__':
    args = arg_factory()
    make_shards(args)
