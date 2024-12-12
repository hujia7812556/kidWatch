import itertools
from abc import ABC
import smbclient
from smbprotocol.exceptions import SMBException
from concurrent.futures import ProcessPoolExecutor
from ..smb.smb_session_pool import SMBSessionPool

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
        self.session_pool = SMBSessionPool(
            host=self.config.get('host'),
            username=self.config.get('username'),
            password=self.config.get('password'),
            port=self.config.get('port'),
            max_sessions=self.config.get('max_sessions', 10)
        )

    def _get_full_path(self, path):
        """构建完整的SMB路径"""
        return f"{self._host}/{self._shared_folder}/{path}"

    def list_video_files(self, path=''):
        """列出目录及子目录下的所有video文件"""
        session = None
        try:
            session = self.session_pool.get_session()
            return self._list_video_files(path)
        except Exception as e:
            print(f"列出视频文件失败: {str(e)}")
            if session:
                self.session_pool.unregister_session(session)
            raise
        finally:
            if session:
                self.session_pool.return_session(session)

    def list_files(self, path='', excludes=[]):
        """列出目录下的所有文件，不包括子目录"""
        session = None
        try:
            session = self.session_pool.get_session()
            files = smbclient.scandir(self._get_full_path(path), port=self._port)
            file_list = []
            for file in files:
                if file.name in excludes:
                    continue
                file_list.append(file.name)
            return file_list
        except Exception as e:
            print(f"列出文件失败: {str(e)}")
            if session:
                self.session_pool.unregister_session(session)
            raise
        finally:
            if session:
                self.session_pool.return_session(session)

    def read(self, path, mode='rb'):
        """读取文件内容"""
        session = None
        try:
            session = self.session_pool.get_session()
            with smbclient.open_file(self._get_full_path(path), mode=mode, port=self._port) as file:
                return file.read()
        except Exception as e:
            print(f"读取文件失败: {str(e)}")
            if session:
                self.session_pool.unregister_session(session)
            raise
        finally:
            if session:
                self.session_pool.return_session(session)

    def path_exists(self, path):
        """检查路径是否存在"""
        session = None
        try:
            session = self.session_pool.get_session()
            smbclient.stat(self._get_full_path(path), port=self._port)
            return True
        except Exception as e:
            print(f"检查路径存在失败: {str(e)}")
            return False
        finally:
            if session:
                self.session_pool.return_session(session)

    def _list_video_files(self, path):
        """内部方法：递归列出视频文件
        
        Args:
            path: 要遍历的路径
        """
        try:
            files = smbclient.scandir(self._get_full_path(path), port=self._port)
            file_list = []
            for file in files:
                if file.name.startswith('.') or file.name.startswith('@'):
                    continue
                if file.is_dir():
                    sub_file_list = self._list_video_files(f"{path}/{file.name}")
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
        return self.session_pool.get_safe_sessions_limit()
