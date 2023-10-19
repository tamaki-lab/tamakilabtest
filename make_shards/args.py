import argparse


def arg_factory():
    parser = argparse.ArgumentParser()

    parser.add_argument('-d', '--dataset_path', action='store',
                        default='/mnt/NAS-TVS872XT/dataset/ActivityNet/v1-3/train_val/',
                        help='Path to the dataset dir with category subdirs.')
    parser.add_argument('-s', '--shard_path', action='store',
                        default='/mnt/SSD4TB/dataset/Activitynet-captions/shards/train',
                        help='Path to the dir to store shard tar files.')
    parser.add_argument('-p', '--shard_prefix', action='store',
                        default='Activitynet_cap',
                        help='Prefix of shard tar files.')
    parser.add_argument('-q', '--quality', type=int, default=80,
                        help='Qualify factor of JPEG file. '
                        'default to 80.')
    parser.add_argument('--max_size_gb', type=float, default=10.0,
                        help='Max size [GB] of each shard tar file. '
                        'default to 10.0 [GB].')
    parser.add_argument('--max_count', type=int, default=100000,
                        help='Max number of entries in each shard tar file. '
                        'default to 100,000.')

    parser.add_argument('--shuffle', dest='shuffle', action='store_true',
                        help='use shuffle')
    parser.add_argument('--no_shuffle', dest='shuffle', action='store_false',
                        help='do not use shuffle')
    parser.set_defaults(shuffle=True)

    parser.add_argument('-w', '--num_workers', type=int, default=8,
                        help='Number of workers. '
                        'default to 8.')
    parser.add_argument('-ss', '--short_side_size', type=int, default=360,
                        help='Shorter side of resized frames. '
                        'default to 360.')
    parser.add_argument('-jp', '--json_path', action='store',
                        default='/mnt/NAS-TVS872XT/dataset/ActivityNet-Captions/captions/train.formatted.json',
                        help='Path to the anotation dir')
    parser.add_argument('--subset', action='store',
                        default='training',
                        help='subset name')

    args = parser.parse_args()
    return args
