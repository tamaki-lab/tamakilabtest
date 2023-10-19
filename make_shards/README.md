# make shards
## Prepare datasets
Execute `make_shards.py`

`make_shards.py` option is below
- `-d`, `--dataset_path`: Path to ActivityNet-Captions video dir
- `-s`, `--shard_path`: Path to the dir to store shard tar files
- `-p`, `--shard_prefix`: Prefix of shard tar files
- `-q`, `--quality`: Qualify factor of JPEG file
- `--max_size_gb`: Max size GB of each shard tar file
- `--max_count`: Max number of entries in each shard tar file
- `--shuffle`: When making shards, video is shuffled
- `--no_shuffle`: When making shards, video is not shuffled
- `-w`, `--num_workers`: Number of workers
- `-ss`, `--short_side_scale`: Shorter side of resized frames
- `-jp`, `--json_path`: Path to ActivityNet-Captions anotation file

### Example
```
python make_shards.py -d ~/datasets/ActivityNet/v1-3/train_val/ -s ~/shards/ActivityNet-Captions/train -jp ~/datasets/ActivityNet-Captions/captions/train.formatted.json
```
