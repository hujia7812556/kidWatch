from threading import Lock
from queue import Queue
from typing import Optional, Set
from .smb_session import SMBSession

class SMBSessionPool:
    def __init__(self, host, username, password, port, max_sessions=10):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.max_sessions = max_sessions
        
        self.session_queue = Queue()
        self.active_sessions: Set[SMBSession] = set()
        self.lock = Lock()
        
        # 初始化一个会话以验证连接
        self._create_and_register_session()
    
    def _create_and_register_session(self) -> Optional[SMBSession]:
        """创建并注册新的SMB会话"""
        try:
            session = SMBSession(self.host, self.username, self.password, self.port)
            self.register_session(session)
            return session
        except Exception as e:
            print(f"创建SMB会话失败: {str(e)}")
            raise

    def register_session(self, session: SMBSession) -> None:
        """注册会话到会话池"""
        with self.lock:
            if session not in self.active_sessions:
                self.active_sessions.add(session)
                self.session_queue.put(session)

    def unregister_session(self, session: SMBSession) -> None:
        """从会话池中注销会话"""
        with self.lock:
            if session in self.active_sessions:
                self.active_sessions.remove(session)
                # 清理队列中的会话
                temp_queue = Queue()
                while not self.session_queue.empty():
                    s = self.session_queue.get()
                    if s != session:
                        temp_queue.put(s)
                self.session_queue = temp_queue
    
    def get_session(self) -> SMBSession:
        """从会话池获取一个会话"""
        with self.lock:
            if self.session_queue.empty() and len(self.active_sessions) < self.max_sessions:
                return self._create_and_register_session()
            
            session = self.session_queue.get()
            # 检查会话是否有效，如果无效则创建新会话
            try:
                if not session.is_connected():
                    self.unregister_session(session)
                    session = self._create_and_register_session()
            except:
                self.unregister_session(session)
                session = self._create_and_register_session()
            
            return session
    
    def return_session(self, session: SMBSession) -> None:
        """归还会话到会话池"""
        with self.lock:
            if session in self.active_sessions:
                self.session_queue.put(session)
    
    def get_available_sessions(self) -> int:
        """获取当前可用的会话数"""
        return self.session_queue.qsize()
    
    def get_safe_sessions_limit(self) -> int:
        """获取安全的并发限制数"""
        return max(1, self.max_sessions - 1) 