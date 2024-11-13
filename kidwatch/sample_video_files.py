import argparse
import csv
import random
from .utils import FileHandlerFactory
from .utils import ConfigReader


class SampleVideoFiles:
    def __init__(self):
        method = ConfigReader().get_config('nas_connect_method')
        self.file_handler = FileHandlerFactory.get_file_handler(method)

    # 采样
    def sample_video_files(self, outfile):
        files = self.file_handler.list_video_files(path='')

        random.seed(20240819)
        sampled_files = random.sample(files, 1000)
        project_path = ConfigReader.get_root_path()
        filename = f'{project_path}/data/intermediate/{outfile}'
        # 打开文件并写入数据
        with open(filename, mode='w', newline="", encoding='utf-8') as file:
            writer = csv.writer(file)
            for row in sampled_files:
                # 写入数组中的每一行
                writer.writerow([row])


if __name__ == "__main__":
    file_op = SampleVideoFiles()
    parser = argparse.ArgumentParser(description='根据输入参数执行不同的方法')

    # 添加参数
    parser.add_argument('-o', '--outfile', type=str,
                        default='sample_video_files.csv',
                        help="输出文件相对路径，文件保存在data目录下")

    # 解析参数
    args = parser.parse_args()
    file_op.sample_video_files(args.outfile)
