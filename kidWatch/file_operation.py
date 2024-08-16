from utils import FileHandlerFactory


class FileOperation:
    def __init__(self, method):
        self.method = method
        self.file_handler = FileHandlerFactory.get_file_handler(method)

    def list_video_files(self, path=''):
        self.file_handler.list_video_files(path=path)


if __name__ == "__main__":
    file_op = FileOperation(method='smb')
    file_op.list_video_files(path='卧室的摄像头')
