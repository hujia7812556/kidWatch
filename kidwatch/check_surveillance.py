import argparse
from datetime import datetime, timedelta
import requests
from .utils import ConfigReader, FileHandlerFactory

class SurveillanceChecker:
    def __init__(self):
        self.config_reader = ConfigReader()
        method = self.config_reader.get_config('nas_connect_method')
        self.file_handler = FileHandlerFactory.get_file_handler(method)
        self.notify_config = self.config_reader.get_config('notify')

    def get_camera_files_count(self, camera, date_str):
        """获取指定摄像头在指定日期的文件数量"""
        am_path = f"{camera}/{date_str}AM"
        pm_path = f"{camera}/{date_str}PM"
        print(f"Checking {am_path} and {pm_path}")
        
        am_exists = self.file_handler.path_exists(am_path)
        pm_exists = self.file_handler.path_exists(pm_path)
        
        if not am_exists and not pm_exists:
            return 0
        
        total_files = 0
        if am_exists:
            try:
                am_files = len(self.file_handler.list_video_files(am_path))
                total_files += am_files
            except FileNotFoundError:
                print(f"Error accessing AM directory: {am_path}")
        
        if pm_exists:
            try:
                pm_files = len(self.file_handler.list_video_files(pm_path))
                total_files += pm_files
            except FileNotFoundError:
                print(f"Error accessing PM directory: {pm_path}")
        
        return total_files

    def find_last_files_date(self, camera, start_date):
        """查找最近一次有文件的日期，最多往前查30天"""
        current_date = start_date
        for i in range(30):
            date_str = current_date.strftime('%Y%m%d')
            files_count = self.get_camera_files_count(camera, date_str)
            if files_count > 0:
                return current_date, files_count
            current_date -= timedelta(days=1)
        return None, 0

    def check_yesterday_files(self):
        # 获取昨天的日期
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime('%Y%m%d')
        
        # 检查所有摄像头目录
        cameras = self.file_handler.list_files(path='', excludes=['.DS_Store'])
        
        for camera in cameras:
            # 获取昨天的文件数
            yesterday_files = self.get_camera_files_count(camera, yesterday_str)
            
            # 如果昨天没有文件，查找最近一次有文件的日期
            if yesterday_files == 0:
                last_date, last_files = self.find_last_files_date(camera, yesterday - timedelta(days=1))
                last_date_str = last_date.strftime('%Y%m%d') if last_date else "未找到"
                self._send_notification(yesterday_str, yesterday_files, [camera], last_date_str, last_files)

    def _send_notification(self, date, total_files, missing_cameras, last_date_str, last_files):
        url = self.notify_config['url']
        headers = {
            'Content-Type': 'application/json',
            'X-API-Token': self.notify_config['api_token']
        }
        
        missing_cameras_str = '、'.join(missing_cameras)
        content = (f"监控同步nas中断\n"
                  f"异常摄像头：{missing_cameras_str}\n"
                  f"日期：{date}\n"
                  f"当天文件数：{total_files}\n"
                  f"最近有文件日期：{last_date_str}\n"
                  f"最近日期文件数：{last_files}")
        
        data = {
            "platform": "wechat",
            "summary": "监控同步nas中断",
            "content": content,
            "extra": {
                "user_id": self.notify_config['user_id']
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            print(f"通知发送成功: {response.status_code}， 通知内容：{content}")
        except requests.exceptions.RequestException as e:
            print(f"通知发送失败: {str(e)}， 通知内容：{content}")

if __name__ == "__main__":
    checker = SurveillanceChecker()
    checker.check_yesterday_files() 