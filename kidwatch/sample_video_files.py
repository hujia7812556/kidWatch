import argparse
import csv
import random
from collections import defaultdict
from .utils.base_handler import BaseHandler
from .utils.config_reader import ConfigReader


class SampleVideoFiles(BaseHandler):
    def __init__(self):
        super().__init__()
        self.camera_configs = self.config_reader.get_config('cameras')

    def get_camera_type(self, video_path):
        """根据视频路径判断摄像头类型"""
        for camera_type, config in self.camera_configs.items():
            if camera_type != 'default':
                folder = config['folder']
                if folder and folder in video_path:
                    return camera_type
        return 'default'

    # 采样
    def sample_video_files(self, outfile):
        """
        按摄像头分别采样视频文件，采样数量从配置文件读取
        """
        # 获取所有视频文件
        all_files = self.file_handler.list_video_files(path='')
        
        # 按摄像头分类视频
        camera_files = defaultdict(list)
        for file_path in all_files:
            camera_type = self.get_camera_type(file_path)
            if camera_type != 'default':
                camera_files[camera_type].append(file_path)
        
        # 采样并记录结果
        sampled_files = []
        random.seed(20240819)  # 固定随机种子以确保可重复性
        
        for camera_type, files in camera_files.items():
            config = self.camera_configs[camera_type]
            sample_size = config.get('sample_size', 0)
            
            if sample_size > 0:
                # 确保采样数量不超过实际文件数量
                actual_sample_size = min(sample_size, len(files))
                camera_samples = random.sample(files, actual_sample_size)
                sampled_files.extend(camera_samples)
                
                camera_name = config['name']
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
    file_op = SampleVideoFiles()
    parser = argparse.ArgumentParser(description='采样视频文件用于训练和评估')

    parser.add_argument('-o', '--outfile', type=str,
                        default='sample_video_files.csv',
                        help="输出文件相对路径，文件保存在data/intermediate目录下")

    args = parser.parse_args()
    file_op.sample_video_files(args.outfile)
