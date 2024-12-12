from threading import Lock
from smbclient import register_session
from queue import Queue
import time

class SMBConnection:
    def __init__(self, host, username, password, port):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.created_time = time.time()
        self.register()
    
    def register(self):
        register_session(self.host, username=self.username, password=self.password, port=self.port)

class SMBConnectionPool:
    def __init__(self, host, username, password, port, max_connections=10):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.max_connections = max_connections
        
        self.connection_queue = Queue()
        self.lock = Lock()
        
        # 初始化连接池
        for _ in range(max_connections):
            self._create_connection()
    
    def _create_connection(self):
        """创建新的SMB连接"""
        try:
            conn = SMBConnection(self.host, self.username, self.password, self.port)
            self.connection_queue.put(conn)
        except Exception as e:
            print(f"创建SMB连接失败: {str(e)}")
            raise
    
    def get_connection(self):
        """从连接池获取一个连接"""
        with self.lock:
            if self.connection_queue.empty():
                self._create_connection()
            return self.connection_queue.get()
    
    def return_connection(self, conn):
        """归还连接到连接池"""
        with self.lock:
            self.connection_queue.put(conn)
    
    def get_available_connections(self):
        """获取当前可用的连接数"""
        return self.connection_queue.qsize()
    
    def get_safe_connections_limit(self):
        """获取安全的并发限制数
        返回当前可用连接数-1，确保至少保留一个连接用于其他操作"""
        # return max(1, min(self.get_available_connections(), int(self.max_connections * 0.6)))  # 至少返回1，避免并发数为0
        return max(1, min(self.get_available_connections(), self.max_connections - 1))
