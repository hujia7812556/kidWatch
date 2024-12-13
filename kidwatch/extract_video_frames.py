import argparse
import itertools
import os
import cv2
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils.base_handler import BaseHandler
import pandas as pd
from queue import Queue, Empty
from threading import Semaphore, Lock


class ExtractVideoFrames(BaseHandler):
    def __init__(self):
        super().__init__()
        # 创建信号量，限制并发数为session池的安全限制数
        self.smb_semaphore = Semaphore(self.file_handler.get_safe_connections_limit())

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
                # self.log_print(f"从 {remote_file_path} 提取了 {frames_count} 帧")
            except Exception as e:
                self.log_print(f"处理 {remote_file_path} 时出错: {str(e)}")
        
        self.log_print(f"总共提取了 {total_frames} 帧")

    def concurrent_download_video_frames(self, camera=None, date=None, video_list_path=None):
        """并发下载视频帧
        
        Args:
            camera: 摄像头配置key
            date: 日期字符串
            video_list_path: 视频列表文件路径
        """
        # 获取视频文件列表
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

        # 获取并发参数
        connection_limit = self.file_handler.get_safe_connections_limit()
        cpu_count = os.cpu_count() or 4
        max_workers = min(
            connection_limit // 2,  # 使用SMB连接池的安全限制数
            cpu_count * 2,    # CPU核心数的2倍
            len(remote_file_paths),  # 不超过文件数
            10  # 硬上限
        )
        self.log_print(f"使用线程数: {max_workers}, SMB连接限制: {connection_limit}")

        # 初始化任务队列和结果统计
        task_queue = Queue()
        for file_path in remote_file_paths:
            task_queue.put(file_path)
        
        processed_count = 0
        total_count = len(remote_file_paths)
        failed_videos = []
        total_frames = 0
        batch_size = 10  # 每次分配的文件数
        results_lock = Lock()  # 用于保护结果统计的锁

        def process_batch():
            """处理一批文件"""
            batch_results = {
                'processed': 0,
                'frames': 0,
                'failed': []
            }

            while True:
                # 获取一批任务
                batch = []
                for _ in range(batch_size):
                    try:
                        file_path = task_queue.get_nowait()
                        batch.append(file_path)
                    except Empty:
                        break
                
                if not batch:  # 没有更多任务了
                    break

                # 处理这批文件
                for video_path in batch:
                    try:
                        frames_count = self.capture_frames_with_semaphore(video_path, output_dir)
                        batch_results['frames'] += frames_count
                        batch_results['processed'] += 1
                        # self.log_print(f"处理视频 {video_path}: 提取了 {frames_count} 帧")
                    except Exception as e:
                        batch_results['failed'].append((video_path, str(e)))
                        self.log_print(f"处理视频 {video_path} 失败: {str(e)}")
                    finally:
                        task_queue.task_done()

                # 更新总体进度
                with results_lock:
                    nonlocal processed_count, total_frames
                    processed_count += batch_results['processed']
                    total_frames += batch_results['frames']
                    failed_videos.extend(batch_results['failed'])
                    progress = (processed_count / total_count) * 100
                    self.log_print(f"总体进度: {progress:.1f}% ({processed_count}/{total_count})")

            return batch_results

        # 使用线程池处理文件
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_batch) for _ in range(max_workers)]
            
            # 等待所有任务完成
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.log_print(f"处理批次时发生错误: {str(e)}")

        # 输出最终统计信息
        self.log_print("\n=== 处理完成 ===")
        self.log_print(f"总视频数: {total_count}")
        self.log_print(f"成功处理: {total_count - len(failed_videos)}")
        self.log_print(f"失败数量: {len(failed_videos)}")
        self.log_print(f"总提取帧数: {total_frames}")
        
        if failed_videos:
            self.log_print("\n失败的视频:")
            for video_path, error in failed_videos:
                self.log_print(f"{video_path}: {error}")

        # 如果失败太多，抛出异常
        if len(failed_videos) > total_count * 0.3:  # 失败率超过30%
            raise RuntimeError(f"处理失败率过高: {len(failed_videos)}/{total_count}")

    def list_video_files(self, camera=None, date=None):
        """列出符合条件的视频文件
        
        Args:
            camera: 摄像头配置key，如 'bedroom', 'living_room' 等
            date: 日期字符串，格式如：20240101
        """
        if camera and camera not in self.camera_configs:
            raise ValueError(f'{camera} is not a valid camera type in config')
            
        # 获取要处理的摄像头文件夹路径列表
        if camera:
            camera_folders = [self.camera_configs[camera]['folder']]
        else:
            # 如果未指定camera，获取所有置的摄像头文件夹（除了default）
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