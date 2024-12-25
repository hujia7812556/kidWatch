import argparse
import itertools
import os
import cv2
import tempfile
import threading
import asyncio
import aiofiles
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
        # 异步模式的信号量
        self._async_semaphore = None
        # 从配置文件获取视频帧处理参数
        self.video_frames_config = self.config_reader.get_config().get('video_frames', {})
        # 获取不同模式的配置
        self.concurrent_config = self.video_frames_config.get('concurrent_mode', {})
        self.async_config = self.video_frames_config.get('async_mode', {})
        # 获取共用的内存限制
        self.max_memory_gb = self.video_frames_config.get('max_memory_gb', 1.5)
        # 获取帧存储路径
        frames_path = self.video_frames_config.get('frames_path', 'data/raw/frames')
        self.output_dir = os.path.join(self.config_reader.get_root_path(), frames_path)

    @property
    def async_semaphore(self):
        """懒加载异步信号量"""
        if self._async_semaphore is None:
            # 使用异步模式的并发数限制
            async_max_workers = self.async_config.get('max_workers', 2)
            max_workers = min(
                async_max_workers,
                self.file_handler.get_safe_connections_limit() // 2,  # SMB连接池限制
            )
            self._async_semaphore = asyncio.Semaphore(max_workers)
            self.log_print(f"使用协程数: {max_workers}, "
                           f"SMB连接限制: {self.file_handler.get_safe_connections_limit()}, "
                           f"批处理大小: {self.concurrent_config.get('batch_size', 10)}")
        return self._async_semaphore

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
            
        # 清空输出目录
        self.clear_frames_directory(self.output_dir)
        
        # 初始化计数器
        total_count = len(remote_file_paths)
        processed_count = 0
        failed_videos = []
        total_frames = 0
        
        for remote_file_path in remote_file_paths:
            try:
                frames_count = self.capture_frames(remote_file_path, self.output_dir)
                total_frames += frames_count
                processed_count += 1
                # 输出进度
                progress = (processed_count / total_count) * 100
                self.log_print(f"总体进度: {progress:.1f}% ({processed_count}/{total_count})")
            except Exception as e:
                failed_videos.append((remote_file_path, str(e)))
                self.log_print(f"处理 {remote_file_path} 时出错: {str(e)}")
                processed_count += 1
        
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
            
        # 清空输出目录
        self.clear_frames_directory(self.output_dir)

        # 使用并发模式的配置参数
        concurrent_max_workers = self.concurrent_config.get('max_workers', 2)
        max_workers = min(
            concurrent_max_workers,
            self.file_handler.get_safe_connections_limit() // 2,  # SMB连接池限制
            len(remote_file_paths)  # 不超过文件数
        )
        self.log_print(f"使用线程数: {max_workers}, SMB连接限制: {self.file_handler.get_safe_connections_limit()}, 批处理大小: {self.concurrent_config.get('batch_size', 10)}")

        # 初始化任务队列和结果统计
        task_queue = Queue()
        for file_path in remote_file_paths:
            task_queue.put(file_path)
        
        processed_count = 0
        total_count = len(remote_file_paths)
        failed_videos = []
        total_frames = 0
        # 使用并发模式的批处理大小
        batch_size = self.concurrent_config.get('batch_size', 10)
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
                        frames_count = self.capture_frames_with_semaphore(video_path, self.output_dir)
                        batch_results['frames'] += frames_count
                        batch_results['processed'] += 1
                    except Exception as e:
                        batch_results['failed'].append((video_path, str(e)))
                        self.log_print(f"处理视频 {video_path} 失败: {str(e)}")
                    finally:
                        task_queue.task_done()

                # 更新总体进度
                with results_lock:
                    nonlocal processed_count, total_frames
                    processed_count += len(batch)  # 使用批次大小更新进度，包括成功和失败的
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

    async def async_capture_frames(self, video_path, output_dir):
        """异步方式从视频中提取帧
        
        Args:
            video_path: 视频文件路径
            output_dir: 输出目录
        Returns:
            int: 提取的帧数
        """
        async with self.async_semaphore:
            camera_type = self.get_camera_type(video_path)
            sample_interval = self.camera_configs[camera_type]['sample_interval']
            
            # 为当前视频创建单独的文件夹
            base_name = os.path.splitext(os.path.basename(video_path))[0]
            video_frame_dir = os.path.join(output_dir, base_name)
            os.makedirs(video_frame_dir, exist_ok=True)
            
            # 创建临时文件来存储视频数据
            async with aiofiles.tempfile.NamedTemporaryFile(suffix='.mp4', delete=True) as temp_file:
                # 从NAS异步读取视频数据到临时文件
                video_data = await self.file_handler.async_read(video_path)
                await temp_file.write(video_data)
                await temp_file.flush()
                
                # 由于OpenCV不支持异步操作，使用线程池处理视频帧提取
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, self._process_video_frames,
                                               temp_file.name, video_frame_dir, sample_interval)

    def _process_video_frames(self, video_path, output_dir, sample_interval):
        """在线程池中处理视频帧提取
        
        Args:
            video_path: 视频文件路径
            output_dir: 输出目录
            sample_interval: 采样间隔
        Returns:
            int: 提取的帧数
        """
        cap = cv2.VideoCapture(video_path)
        frame_count = 0
        saved_count = 0
        
        try:
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                
                if frame_count % sample_interval == 0:
                    frame_file = f"{output_dir}/frame_{frame_count}.jpg"
                    cv2.imwrite(frame_file, frame)
                    saved_count += 1
                
                frame_count += 1
        finally:
            cap.release()
            
        return saved_count

    async def async_download_video_frames(self, camera=None, date=None, video_list_path=None):
        """异步方式下载视频帧
        
        Args:
            camera: 摄像头配置key
            date: 日期字符串
            video_list_path: 视频列表文件路径
        """
        if video_list_path:
            remote_file_paths = self.list_video_files_from_file(video_list_path)
            if not remote_file_paths:
                raise FileNotFoundError(f'视频列表文件 {video_list_path} 中未找到有效的视频文件路径')
        else:
            remote_file_paths = self.list_video_files(camera, date)
            
        # 清空输出目录
        self.clear_frames_directory(self.output_dir)
        
        # 初始化计数器
        total_count = len(remote_file_paths)
        processed_count = 0
        failed_videos = []
        total_frames = 0
        
        # 使用异步模式的批处理大小
        batch_size = self.async_config.get('batch_size', 10)
        
        # 处理所有视频文件
        for i in range(0, len(remote_file_paths), batch_size):
            batch = remote_file_paths[i:i + batch_size]
            tasks = []
            
            for video_path in batch:
                task = asyncio.create_task(self._process_single_video(
                    video_path, self.output_dir, processed_count, total_count))
                tasks.append(task)
            
            # 等待当前批次完成
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 处理结果
            for result in batch_results:
                if isinstance(result, Exception):
                    failed_videos.append((batch[len(failed_videos)], str(result)))
                    self.log_print(f"处理视频失败: {str(result)}")
                else:
                    total_frames += result
                processed_count += 1
                
            # 输出进度
            progress = (processed_count / total_count) * 100
            self.log_print(f"总体进度: {progress:.1f}% ({processed_count}/{total_count})")
        
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

    async def _process_single_video(self, video_path, output_dir, processed_count, total_count):
        """处理单个视频文件
        
        Args:
            video_path: 视频文件路径
            output_dir: 输出目录
            processed_count: 已处理数量
            total_count: 总数量
        Returns:
            int: 提取的帧数
        """
        try:
            frames_count = await self.async_capture_frames(video_path, output_dir)
            return frames_count
        except Exception as e:
            raise Exception(f"处理视频 {video_path} 失败: {str(e)}")


if __name__ == "__main__":
    download_video_file = ExtractVideoFrames()
    parser = argparse.ArgumentParser(description='根据输入参数从视频中提取帧')
    
    parser.add_argument('-camera', '--camera', type=str, default=None,
                      help="摄像头配置名称(如：bedroom, living_room， dining_room)，处理指定摄像头的视频文件，若不设置则处理所有摄像头")
    parser.add_argument('-date', '--date', type=str, default=None,
                      help="日期，格式如：20240101，处理指定日期的视频文件，若不设置则不限日期")
    parser.add_argument('-c', '--concurrent', action='store_true',
                      help="是否使用并发处理")
    parser.add_argument('-m', '--mode', type=str, choices=['normal', 'concurrent', 'async'], default='normal',
                      help="处理模式：normal(普通模式)、concurrent(并发模式)、async(异步模式)，默认normal")
    parser.add_argument('-l', '--list', type=str, default=None,
                      help="视频列表文件路径（CSV格式，需包含video_path列），如果提供则优先使用列表文件中的视频")
    
    args = parser.parse_args()
    
    if args.mode == 'concurrent':
        download_video_file.concurrent_download_video_frames(args.camera, args.date, args.list)
    elif args.mode == 'async':
        asyncio.run(download_video_file.async_download_video_frames(args.camera, args.date, args.list))
    else:
        download_video_file.download_video_frames(args.camera, args.date, args.list)