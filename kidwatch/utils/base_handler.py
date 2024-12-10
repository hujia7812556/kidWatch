from .config_reader import ConfigReader
from .fileHandler import FileHandlerFactory
from datetime import datetime, timedelta

class BaseHandler:
    def __init__(self):
        self.config_reader = ConfigReader()
        method = self.config_reader.get_config('nas_connect_method')
        self.file_handler = FileHandlerFactory.get_file_handler(method)
        # 从配置文件读取摄像头配置
        self.camera_configs = self.config_reader.get_config('cameras')
    
    def get_camera_type(self, video_path):
        """
        根据视频路径判断摄像头类型
        
        Args:
            video_path: 视频文件路径
            
        Returns:
            str: 摄像头类型，如果无法匹配则返回'default'
        """
        for camera_type, config in self.camera_configs.items():
            if camera_type != 'default':
                folder = config['folder']
                if folder and folder in video_path:
                    return camera_type
        return 'default'
    
    def get_formatted_datetime(self):
        """Returns current datetime formatted as string"""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def log_print(self, message):
        """Print message with timestamp"""
        print(f"[{self.get_formatted_datetime()}] {message}")