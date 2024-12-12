import argparse
import itertools
import os
import cv2
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils.base_handler import BaseHandler
import pandas as pd
from queue import Queue


class ExtractVideoFrames(BaseHandler):
    def __init__(self):
        super().__init__()
        # 使用连接池的安全并发数来初始化信号量
        self.connection_limit = self.file_handler.get_safe_connections_limit()
        # 添加信号量来控制并发访问
        self.smb_semaphore = threading.Semaphore(self.connection_limit)  # 根据连接池限制设置信号量

    def clear_frames_directory(self, directory):
        """清空frames目录"""
        if os.path.exists(directory):
            for item in os.listdir(directory):
                item_path = os.path.join(directory, item)
                if os.path.isfile(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    import shutil
                    shutil.rmtree(item_path)
        else:
            os.makedirs(directory)

    def capture_frames(self, video_path, output_dir):
        print(f"开始处理视频: {video_path}")
        """从视频中按配置的间隔截取帧"""
        camera_type = self.get_camera_type(video_path)
        sample_interval = self.camera_configs[camera_type]['sample_interval']
        
        # 为当前视频创建单独的文件夹
        base_name = os.path.splitext(os.path.basename(video_path))[0]
        video_frame_dir = os.path.join(output_dir, base_name)
        os.makedirs(video_frame_dir, exist_ok=True)
        
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
                    # 构造输出文件名，保存到视频专属文件夹中
                    frame_file = f"{video_frame_dir}/frame_{frame_count}.jpg"
                    cv2.imwrite(frame_file, frame)
                    saved_count += 1
                
                frame_count += 1
            
            cap.release()
            return saved_count

    def capture_frames_with_semaphore(self, video_path, output_dir):
        """使用信号量保护的帧捕获方法"""
        with self.smb_semaphore:
            return self.capture_frames(video_path, output_dir)

    def list_video_files_from_file(self, video_list_path):
        """从文件中读取视频文件列表
        
        Args:
            video_list_path: 视频列表文件路径，预期是CSV格式，包含video_path列
        Returns:
            list: 视频文件路径列表
        """
        try:
            df = pd.read_csv(video_list_path)
            if 'video_path' not in df.columns:
                raise ValueError("视频列表文件必须包含'video_path'列")
            return df['video_path'].tolist()
        except Exception as e:
            self.log_print(f"读取视频列表文件失败: {str(e)}")
            return []

    def download_video_frames(self, camera=None, date=None, video_list_path=None):
        """下载视频帧到本地
        
        Args:
            camera: 摄像头配置key
            date: 日期字符串
            video_list_path: 视频列表文件路径，如果提供则优先使用列表文件中的视频
        """
        if video_list_path:
            remote_file_paths = self.list_video_files_from_file(video_list_path)
            if not remote_file_paths:
                raise FileNotFoundError(f'视频列表文件 {video_list_path} 中未找到有效的视频文件路径')
        else:
            remote_file_paths = self.list_video_files(camera, date)
            
        root_dir_path = self.config_reader.get_root_path()
        output_dir = f'{root_dir_path}/data/raw/frames'
        
        # 清空输出目录
        self.clear_frames_directory(output_dir)
        
        total_frames = 0
        for remote_file_path in remote_file_paths:
            try:
                frames_count = self.capture_frames(remote_file_path, output_dir)
                total_frames += frames_count
                self.log_print(f"从 {remote_file_path} 提取了 {frames_count} 帧")
            except Exception as e:
                self.log_print(f"处理 {remote_file_path} 时出错: {str(e)}")
        
        self.log_print(f"总共提取了 {total_frames} 帧")

    def concurrent_download_video_frames(self, camera=None, date=None, video_list_path=None):
        """并发下载视频帧"""
        if video_list_path:
            remote_file_paths = self.list_video_files_from_file(video_list_path)
            if not remote_file_paths:
                raise FileNotFoundError(f'视频列表文件 {video_list_path} 中未找到有效的视频文件路径')
        else:
            remote_file_paths = self.list_video_files(camera, date)
            
        root_dir_path = self.config_reader.get_root_path()
        output_dir = f'{root_dir_path}/data/raw/frames'
        
        # 清空输出目录
        self.clear_frames_directory(output_dir)
        

        # 动态计算最优线程数
        cpu_count = os.cpu_count() or 4
        # 使用min确保不会创建过多线程
        max_workers = min(
            self.connection_limit // 2,  # SMB连接池限制
            cpu_count * 2,    # CPU核心数的2倍
            len(remote_file_paths),  # 不超过文件数
            10  # 硬上限
        )
        self.log_print(f"信号量并发数: {self.connection_limit}")
        self.log_print(f"使用线程数: {max_workers}")
        # exit()
        
        # 使用队列来控制内存使用
        video_queue = Queue(maxsize=max_workers * 2)
        result_queue = Queue()
        
        def worker():
            while True:
                try:
                    video_path = video_queue.get()
                    if video_path is None:  # 退出信号
                        break
                    frames_count = self.capture_frames_with_semaphore(video_path, output_dir)
                    result_queue.put((video_path, frames_count, None))  # 成功
                except Exception as e:
                    result_queue.put((video_path, 0, str(e)))  # 失败
                finally:
                    video_queue.task_done()
        
        # 启动工作线程
        threads = []
        for _ in range(max_workers):
            t = threading.Thread(target=worker)
            t.daemon = True
            t.start()
            threads.append(t)
        
        # 提交任务
        for video_path in remote_file_paths:
            video_queue.put(video_path)
        
        # 发送退出信号
        for _ in range(max_workers):
            video_queue.put(None)
        
        # 等待所有任务完成
        total_frames = 0
        failed_videos = []
        processed_count = 0
        
        while processed_count < len(remote_file_paths):
            video_path, frames_count, error = result_queue.get()
            processed_count += 1
            
            if error:
                failed_videos.append((video_path, error))
                self.log_print(f"处理失败 ({processed_count}/{len(remote_file_paths)}): {video_path}")
                self.log_print(f"错误信息: {error}")
            else:
                total_frames += frames_count
                self.log_print(f"处理成功 ({processed_count}/{len(remote_file_paths)}): "
                             f"{video_path} - {frames_count} 帧")
        
        # 等待所有线程结束
        for t in threads:
            t.join()
        
        # 输出统计信息
        self.log_print(f"\n处理完成:")
        self.log_print(f"总视频数: {len(remote_file_paths)}")
        self.log_print(f"成功处理: {len(remote_file_paths) - len(failed_videos)}")
        self.log_print(f"失败数量: {len(failed_videos)}")
        self.log_print(f"总提取帧数: {total_frames}")
        
        if failed_videos:
            self.log_print("\n失败的视频:")
            for video_path, error in failed_videos:
                self.log_print(f"{video_path}: {error}")

    def list_video_files(self, camera=None, date=None):
        """列出符合条件的视频文件
        
        Args:
            camera: 摄像头配置key，如 'bedroom', 'living_room' 等
            date: 日期字符串，格式如：20240101
        """
        if camera and camera not in self.camera_configs:
            raise ValueError(f'{camera} is not a valid camera type in config')
            
        # 获取需要处理的摄像头文件夹路径列表
        if camera:
            camera_folders = [self.camera_configs[camera]['folder']]
        else:
            # 如果未指定camera，获取所有配置的摄像头文件夹（除了default）
            camera_folders = [config['folder'] for key, config in self.camera_configs.items() 
                            if key != 'default' and config.get('folder')]
        
        paths = []
        for folder in camera_folders:
            if not folder:  # 跳过空文件夹配置
                continue
                
            subfiles = self.file_handler.list_files(path=folder, excludes=['.DS_Store'])
            if date:
                paths += [f'{folder}/{subfile}' for subfile in subfiles if subfile.startswith(date)]
            else:
                paths += [f'{folder}/{subfile}' for subfile in subfiles]
        
        if not paths:
            camera_name = self.camera_configs[camera]['name'] if camera else "所有摄像头"
            raise FileNotFoundError(f'摄像头:{camera_name} 日期:{date} 未找到视频文件')
        
        video_files = []
        for path in paths:
            sub_video_files = self.file_handler.list_video_files(path)
            video_files = list(itertools.chain(video_files, sub_video_files))
        return video_files


if __name__ == "__main__":
    download_video_file = ExtractVideoFrames()
    parser = argparse.ArgumentParser(description='根据输入参数从视频中提取帧')
    
    parser.add_argument('-camera', '--camera', type=str, default=None,
                      help="摄像头配置名称(如：bedroom, living_room， dining_room)，处理指定摄像头的视频文件，若不设置则处理所有摄像头")
    parser.add_argument('-date', '--date', type=str, default=None,
                      help="日期，格式如：20240101，处理指定日期的视频文件，若不设置则不限日期")
    parser.add_argument('-c', '--concurrent', action='store_true',
                      help="是否使用并发处理")
    parser.add_argument('-l', '--list', type=str, default=None,
                      help="视频列表文件路径（CSV格式，需包含video_path列），如果提供则优先使用列表文件中的视频")
    
    args = parser.parse_args()
    
    if args.concurrent:
        download_video_file.concurrent_download_video_frames(args.camera, args.date, args.list)
    else:
        download_video_file.download_video_frames(args.camera, args.date, args.list)