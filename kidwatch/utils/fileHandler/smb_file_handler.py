import itertools
from abc import ABC
import smbclient
from smbprotocol.exceptions import SMBException
from concurrent.futures import ProcessPoolExecutor
from ..smb.smb_session_pool import SMBSessionPool
import time
from threading import Lock
import random

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
    file_locks = {}  # 用于存储文件锁
    locks_lock = Lock()  # 用于保护file_locks字典的锁

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

    def _get_file_lock(self, path):
        """获取指定路径的文件锁"""
        with self.locks_lock:
            if path not in self.file_locks:
                self.file_locks[path] = Lock()
            return self.file_locks[path]

    def list_video_files(self, path=''):
        """列出目录及子目录下的所有video文件"""
        session = None
        try:
            session = self.session_pool.get_session()
            # 添加随机延迟
            time.sleep(random.uniform(0.1, 0.3))
            return self._list_video_files(path)
        except Exception as e:
            print(f"列出视频文件失败: {str(e)}")
            raise
        finally:
            if session:
                self.session_pool.return_session(session)

    def list_files(self, path='', excludes=[]):
        """列出目录下的所有文件，不包括子目录"""
        session = None
        try:
            session = self.session_pool.get_session()
            # 添加随机延迟
            time.sleep(random.uniform(0.1, 0.3))
            files = smbclient.scandir(self._get_full_path(path), port=self._port)
            file_list = []
            for file in files:
                if file.name in excludes:
                    continue
                file_list.append(file.name)
            return file_list
        except Exception as e:
            print(f"列出文件失败: {str(e)}")
            raise
        finally:
            if session:
                self.session_pool.return_session(session)

    def read(self, path, mode='rb'):
        """读取文件内容"""
        session = None
        file_lock = self._get_file_lock(path)  # 获取该文件的锁
        
        try:
            session = self.session_pool.get_session()
            with file_lock:  # 使用文件锁
                # 增加重试机制
                for attempt in range(3):  # 最多重试3次
                    try:
                        # 添加随机延迟，避免多个线程同时请求
                        time.sleep(random.uniform(0.1, 0.5))
                        with smbclient.open_file(self._get_full_path(path), mode=mode, port=self._port) as file:
                            data = file.read()
                            return data
                    except Exception as e:
                        if attempt == 2:  # 最后一次尝试
                            raise
                        # print(f"读取文件失败，尝试重试 ({attempt + 2}/3): {str(e)}")
                        time.sleep(random.uniform(1, 2))  # 随机等待1-2秒后重试
        except Exception as e:
            print(f"读取文件失败: {str(e)}")
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
