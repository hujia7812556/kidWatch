from smbclient import register_session, delete_session
import time

class SMBSession:
    def __init__(self, host, username, password, port):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.created_time = time.time()
        self.last_check_time = time.time()
        self.register()
    
    def register(self):
        """注册新会话前先删除旧会话"""
        try:
            delete_session(self.host)
        except Exception:
            pass
        register_session(self.host, username=self.username, 
                        password=self.password, port=self.port)
    
    def close(self):
        """关闭会话"""
        try:
            delete_session(self.host)
        except Exception:
            pass
    
    def is_connected(self) -> bool:
        """检查 SMB 会话是否仍然有效"""
        current_time = time.time()
        if current_time - self.last_check_time < 60:
            return True
            
        try:
            # 重新注册会话
            self.register()
            self.last_check_time = current_time
            return True
        except Exception:
            return False 