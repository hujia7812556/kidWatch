import argparse
import itertools
import os
import cv2
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils.base_handler import BaseHandler


class ExtractVideoFrames(BaseHandler):
    def __init__(self):
        super().__init__()

    def capture_frames(self, video_path, output_dir):
        """从视频中按配置的间隔截取帧"""
        camera_type = self.get_camera_type(video_path)
        sample_interval = self.camera_configs[camera_type]['sample_interval']
        
        # 创建临时文件来存储视频数据
        with tempfile.NamedTemporaryFile(suffix='.mp4') as temp_file:
            # 从NAS读取视频数据到临时文件
            video_data = self.file_handler.read(video_path)
            temp_file.write(video_data)
            temp_file.flush()
            
            # 打开视频文件
            cap = cv2.VideoCapture(temp_file.name)
            frame_count = 0
            saved_count = 0
            
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                
                if frame_count % sample_interval == 0:
                    # 构造输出文件名
                    base_name = os.path.splitext(os.path.basename(video_path))[0]
                    frame_file = f"{output_dir}/{base_name}_frame_{frame_count}.jpg"
                    cv2.imwrite(frame_file, frame)
                    saved_count += 1
                
                frame_count += 1
            
            cap.release()
            return saved_count

    def download_video_frames(self, subdir=None, date=None):
        """下载视频帧到本地"""
        remote_file_paths = self.list_video_files(subdir, date)
        root_dir_path = self.config_reader.get_root_path()
        output_dir = f'{root_dir_path}/data/raw/frames'
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        total_frames = 0
        for remote_file_path in remote_file_paths:
            try:
                frames_count = self.capture_frames(remote_file_path, output_dir)
                total_frames += frames_count
                self.log_print(f"从 {remote_file_path} 提取了 {frames_count} 帧")
            except Exception as e:
                self.log_print(f"处理 {remote_file_path} 时出错: {str(e)}")
        
        self.log_print(f"总共提取了 {total_frames} 帧")

    def concurrent_download_video_frames(self, subdir=None, date=None):
        """并发下载视频帧"""
        remote_file_paths = self.list_video_files(subdir, date)
        root_dir_path = self.config_reader.get_root_path()
        output_dir = f'{root_dir_path}/data/raw/frames'
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        total_frames = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for remote_file_path in remote_file_paths:
                futures.append(
                    executor.submit(self.capture_frames, remote_file_path, output_dir)
                )
            
            for future in as_completed(futures):
                try:
                    frames_count = future.result()
                    total_frames += frames_count
                except Exception as exc:
                    self.log_print(f"下载失败: {exc}")
        
        self.log_print(f"总共提取了 {total_frames} 帧")

    def list_video_files(self, subdir=None, date=None):
        """列出符合条件的视频文件"""
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
    download_video_file = ExtractVideoFrames()
    parser = argparse.ArgumentParser(description='根据输入参数从视频中提取帧')
    
    parser.add_argument('-subdir', '--subdir', type=str, default=None,
                      help="子目录，处理指定子目录视频文件，若不设置则不限子目录")
    parser.add_argument('-date', '--date', type=str, default=None,
                      help="日期，格式如：20240101，处理指定日期的视频文件，若不设置则不限日期")
    parser.add_argument('-c', '--concurrent', action='store_true',
                      help="是否使用并发处理")
    
    args = parser.parse_args()
    
    if args.concurrent:
        download_video_file.concurrent_download_video_frames(args.subdir, args.date)
    else:
        download_video_file.download_video_frames(args.subdir, args.date)