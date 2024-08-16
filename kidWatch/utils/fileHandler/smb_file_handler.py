import asyncio
import itertools
from abc import ABC
import smbclient
from smbprotocol.exceptions import SMBException

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

    def _get_registered(self):
        if self._is_registered:
            return
        try:
            smbclient.register_session(self._host, self._username, self._password, self._port)
            self._is_registered = True
        except SMBException as e:
            print(f"Failed to register SMB session: {e}")
            raise

    def list_video_files(self, path=''):
        self._get_registered()
        file_list = self._list_video_files(path)
        for file in file_list[:100]:
            print(file)


    def _list_video_files(self, path):
        files = smbclient.scandir(rf"\\{self._host}\{self._shared_folder}\{path}", port=self._port)
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
