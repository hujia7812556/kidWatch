import argparse
from datetime import datetime, timedelta
import requests
from .utils.base_handler import BaseHandler

class CheckerSurveillance(BaseHandler):
    def __init__(self):
        super().__init__()
        self.notify_config = self.config_reader.get_config('notify')

    def get_camera_files_count(self, camera, date_str):
        """获取指定摄像头在指定日期的文件数量"""
        am_path = f"{camera}/{date_str}AM"
        pm_path = f"{camera}/{date_str}PM"
        # print(f"Checking {am_path} and {pm_path}")
        
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
                self.log_print(f"Error accessing AM directory: {am_path}")
        
        if pm_exists:
            try:
                pm_files = len(self.file_handler.list_video_files(pm_path))
                total_files += pm_files
            except FileNotFoundError:
                self.log_print(f"Error accessing PM directory: {pm_path}")
        
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

    def check_files(self, check_date=None):
        """检查指定日期或昨天的文件"""
        if check_date:
            try:
                target_date = datetime.strptime(check_date, '%Y%m%d')
                next_date = target_date + timedelta(days=1)
            except ValueError:
                self.log_print(f"日期格式错误，应为YYYYMMDD，例如：20240301")
                return
        else:
            next_date = datetime.now()
            target_date = next_date - timedelta(days=1)

        target_date_str = target_date.strftime('%Y%m%d')
        next_date_str = next_date.strftime('%Y%m%d')
        
        # 检查所有摄像头目录
        cameras = self.file_handler.list_files(path='', excludes=['.DS_Store'])
        
        for camera in cameras:
            # 获取目标日期的文件数
            target_files = self.get_camera_files_count(camera, target_date_str)
            
            # 如果目标日期有文件，继续检查下一个摄像头
            if target_files > 0:
                self.log_print(f"摄像头：{camera} 同步nas正常，{target_date_str}文件数：{target_files}")
                continue
                
            # 检查下一天的文件数
            next_day_files = self.get_camera_files_count(camera, next_date_str)
            
            # 如果下一天已经有文件了，说明问题已修复，继续检查下一个摄像头
            if next_day_files > 0:
                self.log_print(f"摄像头：{camera} 同步nas已恢复正常，{next_date_str}文件数：{next_day_files}")
                continue
            
            self.log_print(f"摄像头：{camera} 同步nas中断，{target_date_str}文件数：{target_files}")
            # 如果目标日期和下一天都没有文件，查找最近一次有文件的日期并发送通知
            last_date, last_files = self.find_last_files_date(camera, target_date - timedelta(days=1))
            last_date_str = last_date.strftime('%Y%m%d') if last_date else "未找到"
            self._send_notification(target_date_str, target_files, [camera], last_date_str, last_files)

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
                "topic_id": self.notify_config['topic_id']
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            self.log_print(f"通知发送成功: {response.status_code}， 通知内容：{content}")
        except requests.exceptions.RequestException as e:
            self.log_print(f"通知发送失败: {str(e)}， 通知内容：{content}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='检查监控文件同步状态')
    parser.add_argument('-d', '--date', type=str, 
                       help='指定检查日期，格式为YYYYMMDD，例如：20240301。不指定则检查昨天的文件。')
    
    args = parser.parse_args()
    checker = CheckerSurveillance()
    checker.check_files(args.date) 
