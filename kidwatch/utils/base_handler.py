from .config_reader import ConfigReader
from .fileHandler import FileHandlerFactory
from datetime import datetime, timedelta

class BaseHandler:
    def __init__(self):
        self.config_reader = ConfigReader()
        method = self.config_reader.get_config('nas_connect_method')
        self.file_handler = FileHandlerFactory.get_file_handler(method)
    
    def get_formatted_datetime(self):
        """Returns current datetime formatted as string"""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def log_print(self, message):
        """Print message with timestamp"""
        print(f"[{self.get_formatted_datetime()}] {message}")