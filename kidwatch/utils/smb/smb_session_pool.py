from threading import Lock
from queue import Queue
from typing import Optional
from .smb_session import SMBSession

class SMBSessionPool:
    def __init__(self, host, username, password, port, max_sessions=10):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.max_sessions = max_sessions
        
        self.session_queue = Queue()
        self.lock = Lock()
        self.created_sessions = 0  # 跟踪已创建的会话数量
        
        # 初始化一个默认会话
        try:
            default_session = SMBSession(self.host, self.username, self.password, self.port)
            self.session_queue.put(default_session)
            self.created_sessions = 1
        except Exception as e:
            print(f"初始化默认SMB会话失败: {str(e)}")
            raise
    
    def _create_new_session(self) -> SMBSession:
        """创建新的SMB会话"""
        try:
            return SMBSession(self.host, self.username, self.password, self.port)
        except Exception as e:
            print(f"创建SMB会话失败: {str(e)}")
            raise

    def get_session(self) -> SMBSession:
        """从会话池获取一个会话，如果需要则创建新会话"""
        with self.lock:
            if not self.session_queue.empty():
                session = self.session_queue.get()
                # 检查会话是否有效，如果无效则创建新会话
                try:
                    if not session.is_connected():
                        session = self._create_new_session()
                except:
                    session = self._create_new_session()
            else:
                # 如果没有可用会话且未达到最大限制，创建新会话
                if self.created_sessions < self.max_sessions:
                    session = self._create_new_session()
                    self.created_sessions += 1
                else:
                    # 等待一个会话变得可用
                    session = self.session_queue.get()
                    try:
                        if not session.is_connected():
                            session = self._create_new_session()
                    except:
                        session = self._create_new_session()
            
            return session
    
    def return_session(self, session: SMBSession) -> None:
        """归还会话到会话池"""
        if session.is_connected():
            self.session_queue.put(session)
    
    def get_available_sessions(self) -> int:
        """获取当前可用的会话数"""
        return self.session_queue.qsize()
    
    def get_safe_sessions_limit(self) -> int:
        """获取安全的并发限制数"""
        return max(1, self.max_sessions - 1) 