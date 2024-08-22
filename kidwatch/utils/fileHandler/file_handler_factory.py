from .smb_file_handler import SmbFileHandler
from ..config_reader import ConfigReader


class FileHandlerFactory:
    def __init__(self):
        self.config_reader = ConfigReader()

    @staticmethod
    def get_file_handler(method):
        if method == 'smb':
            return SmbFileHandler()
        else:
            raise ValueError(f"Unsupported method: {method}")