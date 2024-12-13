from smbclient import register_session, delete_session, scandir
import time
import threading

class SMBSession:
    def __init__(self, host, username, password, port):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.created_time = time.time()
        self.last_check_time = time.time()
        self.is_alive = True
        self.register()
        
        # 启动心跳线程
        self.heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.heartbeat_thread.start()
    
    def register(self):
        """注册新会话前先删除旧会话"""
        try:
            delete_session(self.host)
        except Exception:
            pass
        register_session(self.host, username=self.username, 
                        password=self.password, port=self.port)
        self.last_check_time = time.time()
    
    def _heartbeat(self):
        """心跳检测，每30秒检查一次会话状态"""
        while self.is_alive:
            try:
                # 尝试列出根目录，验证会话是否有效
                scandir(f"//{self.host}", port=self.port)
                self.last_check_time = time.time()
            except Exception:
                try:
                    self.register()
                except Exception as e:
                    print(f"心跳重新注册失败: {str(e)}")
            time.sleep(30)  # 每30秒检查一次
    
    def close(self):
        """关闭会话"""
        self.is_alive = False  # 停止心跳线程
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