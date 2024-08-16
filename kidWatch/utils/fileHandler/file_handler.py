from abc import ABC, abstractmethod


class FileHandler(ABC):

    @abstractmethod
    def list_video_files(self, path=''):
        pass

    # @abstractmethod
    # def read(self, path):
    #     pass
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
