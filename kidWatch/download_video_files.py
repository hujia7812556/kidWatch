import argparse
import itertools
import os
import shutil
import tempfile

from utils import FileHandlerFactory
from utils import ConfigReader


class DownloadVideoFiles:
    def __init__(self):
        method = ConfigReader().get_config('nas_connect_method')
        self.file_handler = FileHandlerFactory.get_file_handler(method)

    def download_video_files(self, subdir=None, date=None):
        paths = self.list_video_files(subdir, date)
        project_path = ConfigReader().get_config('project_path')
        download_dirname = f'{project_path}/data/raw/downloads'
        with tempfile.TemporaryDirectory(dir=download_dirname) as temp_dirname:
            print(temp_dirname)
            for path in paths:
                file_name = os.path.basename(path)
                tmp_path = f'{temp_dirname}/{file_name}'
                with open(tmp_path, 'wb') as local_file:
                    local_file.write(self.file_handler.read(path))

            # 清空原来目录
            for file_name in os.listdir(download_dirname):
                if file_name.endswith('.mp4'):
                    # 构造完整的文件路径
                    file_path = os.path.join(download_dirname, file_name)
                    try:
                        # 删除文件
                        os.remove(file_path)
                        print(f'已删除文件: {file_path}')
                    except Exception as e:
                        print(f'删除文件 {file_path} 时出错: {e}')

            # 移动新下载的文件
            for file_name in os.listdir(temp_dirname):
                shutil.move(f'{temp_dirname}/{file_name}', f'{download_dirname}/{file_name}')




    def list_video_files(self, subdir=None, date=None):
        files = self.file_handler.list_files(path='', excludes=['.DS_Store'])
        if subdir and subdir not in files:
            raise FileNotFoundError(f'{subdir} is not a valid subdirectory')
        subdirs = [subdir] if subdir else files
        paths = []
        for tmp_subdir in subdirs:
            subfiles = self.file_handler.list_files(path=tmp_subdir)
            if date:
                paths += [f'{tmp_subdir}/{subfile}' for subfile in subfiles if subfile.startswith(date)]
            else:
                paths += [f'{tmp_subdir}/{subfile}' for subfile in subfiles]
        if not paths:
            raise FileNotFoundError(f'subdir:{subdir} date:{date} is not a valid path')
        video_files = []
        for path in paths:
            sub_video_files = self.file_handler.list_video_files(path)
            video_files = list(itertools.chain(video_files, sub_video_files))
        return video_files


if __name__ == "__main__":
    download_video_file = DownloadVideoFiles()
    parser = argparse.ArgumentParser(description='根据输入参数执行不同的方法')

    # 添加参数
    parser.add_argument('-subdir', '--subdir', type=str, default=None,
                        help="子目录，下载指定子目录视频文件，若不设置则不限子目录")
    parser.add_argument('-date', '--date', type=str, default=None,
                        help="日期，格式如：20240101，下载指定日期的视频文件，若不设置则不限日期")

    # 解析参数
    args = parser.parse_args()
    subdir = args.subdir
    date = args.date
    download_video_file.download_video_files(subdir, date)