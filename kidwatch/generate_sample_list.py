import argparse
import csv
import random
from collections import defaultdict
from .utils.base_handler import BaseHandler

class GenerateSampleList(BaseHandler):
    def __init__(self):
        super().__init__()
        self.camera_configs = self.config_reader.get_config('cameras')

    # 采样
    def generate_sample_list(self, outfile):
        """
        按摄像头分别采样视频文件，采样数量从配置文件读取
        """
        
        # 采样并记录结果
        sampled_files = []
        random.seed(20240819)  # 固定随机种子以确保可重复性
        
        for config in self.camera_configs.values():
            sample_size = config.get('sample_size', 0)
            folder = config.get('folder', '')
            camera_name = config.get('name', '')
            
            if sample_size > 0:
                files = self.file_handler.list_video_files(folder)
                # 确保采样数量不超过实际文件数量
                actual_sample_size = min(sample_size, len(files))
                camera_samples = random.sample(files, actual_sample_size)
                sampled_files.extend(camera_samples)
                
                self.log_print(f"{camera_name}摄像头: 总计 {len(files)} 个视频，"
                             f"计划采样 {sample_size} 个，实际采样 {actual_sample_size} 个")
        
        # 将采样结果写入CSV文件
        project_path = self.config_reader.get_root_path()
        filename = f'{project_path}/data/intermediate/{outfile}'
        
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for file_path in sampled_files:
                writer.writerow([file_path])
        
        self.log_print(f"\n总计采样 {len(sampled_files)} 个视频，结果保存至 {filename}")


if __name__ == "__main__":
    file_op = GenerateSampleList()
    parser = argparse.ArgumentParser(description='采样视频文件用于训练和评估')

    parser.add_argument('-o', '--outfile', type=str,
                        default='sample_video_list.csv',
                        help="输出文件相对路径，文件保存在data/intermediate目录下")

    args = parser.parse_args()
    file_op.generate_sample_list(args.outfile)
