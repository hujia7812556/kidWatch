import itertools
from abc import ABC

import smbclient
from smbprotocol.exceptions import SMBException
from concurrent.futures import ProcessPoolExecutor
from .smb_connection_pool import SMBConnectionPool

from .file_handler import FileHandler
from ..config_reader import ConfigReader


class SmbFileHandler(FileHandler, ABC):
    _instance = None
    _is_registered = None
    _host = None
    _port = None
    _shared_folder = None
    _username = None
    _password = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SmbFileHandler, cls).__new__(cls, *args, **kwargs)
            cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(self):
        self.config = ConfigReader().get_smb_config()
        self._host = self.config.get('host')
        self._port = self.config.get('port')
        self._shared_folder = self.config.get('shared_folder')
        self._username = self.config.get('username')
        self._password = self.config.get('password')
        self.connection_pool = SMBConnectionPool(
            host=self.config.get('host'),
            username=self.config.get('username'),
            password=self.config.get('password'),
            port=self.config.get('port'),
            max_connections=self.config.get('max_connections', 10)
        )

    def list_video_files(self, path=''):
        """列出目录及子目录下的所有video文件"""
        connection = self.connection_pool.get_connection()
        try:
            return self._list_video_files(path, connection)
        finally:
            self.connection_pool.return_connection(connection)

    def list_files(self, path='', excludes=[]):
        """列出目录下的所有文件，不包括子目录"""
        conn_time = self.connection_pool.get_connection()
        try:
            files = smbclient.scandir(f"{self._host}/{self._shared_folder}/{path}", port=self._port)
            file_list = []
            for file in files:
                if file.name in excludes:
                    continue
                file_list.append(file.name)
            return file_list
        finally:
            self.connection_pool.return_connection(conn_time)

    def read(self, path, mode='rb'):
        """读取文件内容"""
        connection = self.connection_pool.get_connection()
        try:
            with smbclient.open_file(f"{self._host}/{self._shared_folder}/{path}", 
                                    mode=mode, port=self._port) as file:
                return file.read()
        finally:
            self.connection_pool.return_connection(connection)

    def path_exists(self, path):
        """检查路径是否存在"""
        connection = self.connection_pool.get_connection()
        try:
            try:
                smbclient.stat(f"{self._host}/{self._shared_folder}/{path}", port=self._port)
                return True
            except:
                return False
        finally:
            self.connection_pool.return_connection(connection)

    def _list_video_files(self, path, connection):
        """内部方法：递归列出视频文件
        
        Args:
            path: 要遍历的路径
            connection: 从连接池获取的连接
        """
        try:
            files = smbclient.scandir(f"{self._host}/{self._shared_folder}/{path}", port=self._port)
            file_list = []
            for file in files:
                if file.name.startswith('.') or file.name.startswith('@'):
                    continue
                if file.is_dir():
                    # 递归调用时传递同一个连接
                    sub_file_list = self._list_video_files(f"{path}/{file.name}", connection)
                    file_list = list(itertools.chain(file_list, sub_file_list))
                else:
                    if file.name.endswith('.mp4'):
                        file_list.append(f"{path}/{file.name}")
            return file_list
        except Exception as e:
            print(f"列出视频文件失败 {path}: {str(e)}")
            return []
        
    def get_safe_connections_limit(self):
        """获取安全的并发限制数"""
        return self.connection_pool.get_safe_connections_limit()

    def _process_single_file(self, remote_path, local_path):
        """处理单个文件的下载"""
        connection = self.connection_pool.get_connection()
        try:
            with smbclient.open_file(remote_path, mode='rb') as remote_file:
                with open(local_path, 'wb') as local_file:
                    local_file.write(remote_file.read())
            return True
        except Exception as e:
            print(f"下载文件失败 {remote_path}: {str(e)}")
            return False
        finally:
            self.connection_pool.return_connection(connection)

    def download_files(self, file_list):
        """并行下载多个文件"""
        with ProcessPoolExecutor(max_workers=self.config.get('max_workers', 4)) as executor:
            futures = []
            for remote_path, local_path in file_list:
                future = executor.submit(self._process_single_file, remote_path, local_path)
                futures.append(future)
            
            results = [future.result() for future in futures]
        return all(results)
