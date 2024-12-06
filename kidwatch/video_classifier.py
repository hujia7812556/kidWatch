import cv2
import numpy as np
import torch
from ultralytics import YOLO
import argparse
import csv
from datetime import datetime
from .utils.base_handler import BaseHandler

class VideoClassifier(BaseHandler):
    def __init__(self):
        super().__init__()
        # 加载预训练的YOLOv8模型
        self.model = YOLO('yolov8n.pt')
        # 从配置文件读取摄像头配置
        self.camera_configs = self.config_reader.get_config('cameras')
        self.person_class_id = 0
        
    def get_camera_type(self, video_path):
        """根据视频路径判断摄像头类型"""
        # 获取所有配置的摄像头类型（除了default）
        camera_types = [cam_type for cam_type in self.camera_configs.keys() 
                       if cam_type != 'default']
        
        # 检查路径中是否包含任何已配置的摄像头文件夹名称
        for camera_type in camera_types:
            folder = self.camera_configs[camera_type]['folder']
            if folder and folder in video_path:
                return camera_type
                
        # 如果没有匹配的摄像头类型，返回默认配置
        self.log_print(f"警告：无法从路径识别摄像头类型 {video_path}，使用默认配置")
        return 'default'
        
    def get_camera_name(self, camera_type):
        """获取摄像头的显示名称"""
        return self.camera_configs[camera_type]['name']
        
    def process_video(self, video_path):
        """
        处理视频文件，检测是否包含小孩
        """
        camera_type = self.get_camera_type(video_path)
        config = self.camera_configs[camera_type]
        
        cap = cv2.VideoCapture(video_path)
        frame_count = 0
        child_detected = False
        
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
                
            if frame_count % config['sample_interval'] == 0:
                # 使用YOLO进行目标检测
                results = self.model(frame, conf=config['conf_threshold'])[0]
                
                # 分析检测结果
                for detection in results.boxes.data:
                    class_id = int(detection[5])
                    confidence = float(detection[4])
                    
                    if class_id == self.person_class_id:
                        # 获取边界框信息
                        bbox = detection[:4].cpu().numpy()
                        height = bbox[3] - bbox[1]
                        
                        # 基于身高判断是否为小孩
                        if height < frame.shape[0] * config['height_ratio']:
                            child_detected = True
                            break
                
                if child_detected:
                    break
            
            frame_count += 1
        
        cap.release()
        return child_detected, camera_type

    def batch_process_videos(self, video_list_file, output_file):
        """
        批量处理视频文件并输出结果
        """
        results = []
        # 初始化统计信息（不包括default配置）
        camera_stats = {camera: {'total': 0, 'with_child': 0} 
                       for camera in self.camera_configs.keys()
                       if camera != 'default'}
        
        with open(video_list_file, 'r') as f:
            video_paths = [line.strip() for line in f.readlines()]
        
        for video_path in video_paths:
            try:
                has_child, camera_type = self.process_video(video_path)
                camera_name = self.get_camera_name(camera_type)
                results.append({
                    'video_path': video_path,
                    'camera_type': camera_type,
                    'camera_name': camera_name,
                    'has_child': has_child,
                    'processed_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                
                # 更新统计信息（只统计非default的摄像头）
                if camera_type != 'default':
                    camera_stats[camera_type]['total'] += 1
                    if has_child:
                        camera_stats[camera_type]['with_child'] += 1
                    
                self.log_print(f"处理视频 {video_path} ({camera_name}): {'有' if has_child else '无'}小孩")
            except Exception as e:
                self.log_print(f"处理视频 {video_path} 时出错: {str(e)}")
        
        # 输出每个摄像头的统计信息
        self.log_print("\n=== 处理统计 ===")
        for camera_type, stats in camera_stats.items():
            if stats['total'] > 0:
                camera_name = self.get_camera_name(camera_type)
                child_ratio = (stats['with_child'] / stats['total']) * 100
                self.log_print(f"{camera_name}摄像头: 总计 {stats['total']} 个视频, "
                             f"包含小孩 {stats['with_child']} 个 ({child_ratio:.1f}%)")
        
        # 将结果写入CSV文件
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, 
                                  fieldnames=['video_path', 'camera_type', 
                                            'camera_name', 'has_child', 
                                            'processed_time'])
            writer.writeheader()
            writer.writerows(results)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='检测视频中是否包含小孩')
    parser.add_argument('-i', '--input', required=True,
                      help='包含视频文件路径的列表文件')
    parser.add_argument('-o', '--output', required=True,
                      help='结果输出文件路径')
    
    args = parser.parse_args()
    classifier = VideoClassifier()
    classifier.batch_process_videos(args.input, args.output) 