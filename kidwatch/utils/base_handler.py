from .config_reader import ConfigReader
from .fileHandler import FileHandlerFactory

class BaseHandler:
    def __init__(self):
        self.config_reader = ConfigReader()
        method = self.config_reader.get_config('nas_connect_method')
        self.file_handler = FileHandlerFactory.get_file_handler(method) 