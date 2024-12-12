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
        register_session(self.host, username=self.username, password=self.password, port=self.port)
    
    def is_connected(self) -> bool:
        """
        检查 SMB 会话是否仍然有效
        
        使用以下策略检查连接状态：
        1. 检查距离上次验证的时间间隔
        2. 如果间隔小于阈值（如60秒），假定连接仍然有效
        3. 如果间隔超过阈值，才进行实际的连接测试
        
        Returns:
            bool: 如果会话有效返回 True，否则返回 False
        """
        current_time = time.time()
        # 如果距离上次检查时间不超过60秒，假定连接仍然有效
        if current_time - self.last_check_time < 60:
            return True
            
        try:
            # 只在超过时间阈值时才进行实际的连接测试
            self.register()
            self.last_check_time = current_time
            return True
        except Exception:
            return False 