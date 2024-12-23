from abc import ABC, abstractmethod


class FileHandler(ABC):

    # 列出目录及子目录下的所有video文件
    @abstractmethod
    def list_video_files(self, path=''):
        pass

    # 列出目录下的所有文件，不包括子目录
    @abstractmethod
    def list_files(self, path='', excludes=[]):
        pass

    @abstractmethod
    def read(self, path, mode='rb'):
        pass

    @abstractmethod
    def path_exists(self, path):
        pass

    @abstractmethod
    async def async_read(self, path, mode='rb'):
        """异步读取文件内容
        
        Args:
            path: 文件路径
            mode: 读取模式，默认为'rb'
        Returns:
            bytes: 文件内容
        """
        pass
