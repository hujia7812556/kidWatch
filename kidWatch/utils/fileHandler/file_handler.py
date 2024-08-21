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
    #
    # @abstractmethod
    # def delete(self, path):
    #     pass
    #
    # @abstractmethod
    # def copy(self, source, destination):
    #     pass
    #
    # @abstractmethod
    # def move(self, source, destination):
    #     pass
