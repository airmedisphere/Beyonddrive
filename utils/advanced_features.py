"""
Advanced Features Module for TG Drive
Contains all enhanced functionality for the file storage system.
"""

import asyncio
import hashlib
import secrets
import json
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Set
from pathlib import Path
from dataclasses import dataclass, field, asdict
from collections import defaultdict
import aiofiles
import aiohttp

from utils.logger import Logger

logger = Logger(__name__)

# ============================================================================
# DATA CLASSES FOR STRUCTURED DATA
# ============================================================================

@dataclass
class User:
    """User account for multi-user support"""
    id: str
    username: str
    password_hash: str
    email: Optional[str] = None
    role: str = "viewer"  # admin, uploader, viewer
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_login: Optional[str] = None
    two_factor_enabled: bool = False
    two_factor_secret: Optional[str] = None
    favorites: List[str] = field(default_factory=list)
    recent_files: List[str] = field(default_factory=list)
    settings: Dict[str, Any] = field(default_factory=dict)

@dataclass
class AccessLog:
    """Log entry for file access tracking"""
    id: str
    user_id: Optional[str]
    file_path: str
    action: str  # view, download, stream, share
    ip_address: str
    user_agent: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    details: Dict[str, Any] = field(default_factory=dict)

@dataclass
class FileVersion:
    """Version tracking for files"""
    version_id: str
    file_id: str
    message_id: int
    size: int
    created_at: str
    created_by: Optional[str] = None
    comment: Optional[str] = None

@dataclass
class Collection:
    """Playlist/Collection of files"""
    id: str
    name: str
    description: str = ""
    owner_id: str = "admin"
    file_paths: List[str] = field(default_factory=list)
    is_public: bool = False
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    cover_image: Optional[str] = None
    tags: List[str] = field(default_factory=list)

@dataclass
class VideoChapter:
    """Chapter/timestamp for video navigation"""
    title: str
    start_time: float  # seconds
    end_time: Optional[float] = None
    thumbnail: Optional[str] = None

@dataclass 
class ScheduledUpload:
    """Scheduled upload task"""
    id: str
    url: str
    destination_path: str
    filename: str
    scheduled_time: str
    status: str = "pending"  # pending, running, completed, failed
    created_by: str = "admin"
    error_message: Optional[str] = None

@dataclass
class BandwidthUsage:
    """Bandwidth tracking per day"""
    date: str
    bytes_streamed: int = 0
    bytes_downloaded: int = 0
    request_count: int = 0
    unique_ips: Set[str] = field(default_factory=set)


# ============================================================================
# USER MANAGEMENT SYSTEM
# ============================================================================

class UserManager:
    """Multi-user authentication and management"""
    
    def __init__(self):
        self.users: Dict[str, User] = {}
        self.sessions: Dict[str, Dict[str, Any]] = {}  # session_token -> {user_id, expires_at}
        self.failed_attempts: Dict[str, List[datetime]] = defaultdict(list)  # IP -> attempts
        self.ip_whitelist: Set[str] = set()
        self.ip_blacklist: Set[str] = set()
        self._init_admin()
    
    def _init_admin(self):
        """Initialize default admin user"""
        admin_id = "admin"
        if admin_id not in self.users:
            self.users[admin_id] = User(
                id=admin_id,
                username="admin",
                password_hash=self._hash_password("admin"),
                role="admin"
            )
    
    def _hash_password(self, password: str) -> str:
        """Hash password with salt"""
        salt = "tgdrive_salt_2024"
        return hashlib.sha256(f"{password}{salt}".encode()).hexdigest()
    
    def _generate_session_token(self) -> str:
        """Generate secure session token"""
        return secrets.token_urlsafe(32)
    
    def _generate_2fa_secret(self) -> str:
        """Generate 2FA secret"""
        return secrets.token_hex(16)
    
    def check_ip_allowed(self, ip: str) -> bool:
        """Check if IP is allowed to access"""
        if self.ip_blacklist and ip in self.ip_blacklist:
            return False
        if self.ip_whitelist and ip not in self.ip_whitelist:
            return False
        return True
    
    def check_brute_force(self, ip: str) -> bool:
        """Check for brute force attempts"""
        now = datetime.now()
        # Clean old attempts (older than 15 minutes)
        self.failed_attempts[ip] = [
            t for t in self.failed_attempts[ip] 
            if now - t < timedelta(minutes=15)
        ]
        # Block if more than 5 attempts in 15 minutes
        return len(self.failed_attempts[ip]) < 5
    
    def record_failed_attempt(self, ip: str):
        """Record a failed login attempt"""
        self.failed_attempts[ip].append(datetime.now())
    
    def create_user(self, username: str, password: str, email: str = None, role: str = "viewer") -> Optional[User]:
        """Create a new user"""
        user_id = secrets.token_hex(8)
        if any(u.username == username for u in self.users.values()):
            return None  # Username exists
        
        user = User(
            id=user_id,
            username=username,
            password_hash=self._hash_password(password),
            email=email,
            role=role
        )
        self.users[user_id] = user
        logger.info(f"Created user: {username} with role {role}")
        return user
    
    def authenticate(self, username: str, password: str, ip: str) -> Optional[str]:
        """Authenticate user and return session token"""
        if not self.check_ip_allowed(ip):
            logger.warning(f"IP blocked: {ip}")
            return None
        
        if not self.check_brute_force(ip):
            logger.warning(f"Brute force detected from: {ip}")
            return None
        
        for user in self.users.values():
            if user.username == username:
                if user.password_hash == self._hash_password(password):
                    # Success
                    token = self._generate_session_token()
                    self.sessions[token] = {
                        "user_id": user.id,
                        "expires_at": (datetime.now() + timedelta(hours=24)).isoformat(),
                        "ip": ip
                    }
                    user.last_login = datetime.now().isoformat()
                    logger.info(f"User logged in: {username}")
                    return token
                else:
                    self.record_failed_attempt(ip)
                    return None
        
        self.record_failed_attempt(ip)
        return None
    
    def validate_session(self, token: str) -> Optional[User]:
        """Validate session token and return user"""
        if token not in self.sessions:
            return None
        
        session = self.sessions[token]
        if datetime.fromisoformat(session["expires_at"]) < datetime.now():
            del self.sessions[token]
            return None
        
        return self.users.get(session["user_id"])
    
    def logout(self, token: str):
        """Logout user"""
        if token in self.sessions:
            del self.sessions[token]
    
    def enable_2fa(self, user_id: str) -> Optional[str]:
        """Enable 2FA for user"""
        if user_id in self.users:
            secret = self._generate_2fa_secret()
            self.users[user_id].two_factor_enabled = True
            self.users[user_id].two_factor_secret = secret
            return secret
        return None
    
    def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get user by ID"""
        return self.users.get(user_id)
    
    def list_users(self) -> List[Dict]:
        """List all users (without sensitive data)"""
        return [
            {
                "id": u.id,
                "username": u.username,
                "email": u.email,
                "role": u.role,
                "created_at": u.created_at,
                "last_login": u.last_login,
                "two_factor_enabled": u.two_factor_enabled
            }
            for u in self.users.values()
        ]
    
    def update_user_role(self, user_id: str, new_role: str) -> bool:
        """Update user role"""
        if user_id in self.users and new_role in ["admin", "uploader", "viewer"]:
            self.users[user_id].role = new_role
            return True
        return False
    
    def delete_user(self, user_id: str) -> bool:
        """Delete user"""
        if user_id in self.users and user_id != "admin":
            del self.users[user_id]
            # Remove sessions
            self.sessions = {k: v for k, v in self.sessions.items() if v["user_id"] != user_id}
            return True
        return False


# ============================================================================
# ACCESS LOGGING & ANALYTICS
# ============================================================================

class AnalyticsManager:
    """Analytics and access logging"""
    
    def __init__(self):
        self.access_logs: List[AccessLog] = []
        self.bandwidth_usage: Dict[str, BandwidthUsage] = {}  # date -> usage
        self.popular_files: Dict[str, int] = defaultdict(int)  # file_path -> access_count
        self.storage_stats: Dict[str, int] = {}  # file_type -> total_size
    
    def log_access(self, file_path: str, action: str, ip: str, user_agent: str, 
                   user_id: str = None, details: Dict = None):
        """Log file access"""
        log_id = secrets.token_hex(8)
        log = AccessLog(
            id=log_id,
            user_id=user_id,
            file_path=file_path,
            action=action,
            ip_address=ip,
            user_agent=user_agent,
            details=details or {}
        )
        self.access_logs.append(log)
        self.popular_files[file_path] += 1
        
        # Keep only last 10000 logs
        if len(self.access_logs) > 10000:
            self.access_logs = self.access_logs[-10000:]
    
    def record_bandwidth(self, bytes_count: int, is_stream: bool, ip: str):
        """Record bandwidth usage"""
        today = datetime.now().strftime("%Y-%m-%d")
        if today not in self.bandwidth_usage:
            self.bandwidth_usage[today] = BandwidthUsage(date=today)
        
        usage = self.bandwidth_usage[today]
        if is_stream:
            usage.bytes_streamed += bytes_count
        else:
            usage.bytes_downloaded += bytes_count
        usage.request_count += 1
        usage.unique_ips.add(ip)
    
    def get_popular_files(self, limit: int = 10) -> List[Dict]:
        """Get most popular files"""
        sorted_files = sorted(self.popular_files.items(), key=lambda x: x[1], reverse=True)
        return [{"path": path, "access_count": count} for path, count in sorted_files[:limit]]
    
    def get_recent_logs(self, limit: int = 100, file_path: str = None, user_id: str = None) -> List[Dict]:
        """Get recent access logs with optional filters"""
        logs = self.access_logs
        if file_path:
            logs = [l for l in logs if l.file_path == file_path]
        if user_id:
            logs = [l for l in logs if l.user_id == user_id]
        return [asdict(l) for l in logs[-limit:]]
    
    def get_bandwidth_stats(self, days: int = 7) -> List[Dict]:
        """Get bandwidth statistics for last N days"""
        result = []
        for i in range(days):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            if date in self.bandwidth_usage:
                usage = self.bandwidth_usage[date]
                result.append({
                    "date": date,
                    "bytes_streamed": usage.bytes_streamed,
                    "bytes_downloaded": usage.bytes_downloaded,
                    "request_count": usage.request_count,
                    "unique_visitors": len(usage.unique_ips)
                })
            else:
                result.append({
                    "date": date,
                    "bytes_streamed": 0,
                    "bytes_downloaded": 0,
                    "request_count": 0,
                    "unique_visitors": 0
                })
        return result
    
    def get_storage_breakdown(self) -> Dict:
        """Get storage usage by file type"""
        return dict(self.storage_stats)
    
    def update_storage_stats(self, file_extension: str, size: int, remove: bool = False):
        """Update storage statistics"""
        ext = file_extension.lower()
        if remove:
            self.storage_stats[ext] = max(0, self.storage_stats.get(ext, 0) - size)
        else:
            self.storage_stats[ext] = self.storage_stats.get(ext, 0) + size


# ============================================================================
# FILE VERSIONING SYSTEM  
# ============================================================================

class VersionManager:
    """File version control"""
    
    def __init__(self):
        self.versions: Dict[str, List[FileVersion]] = defaultdict(list)  # file_id -> versions
    
    def add_version(self, file_id: str, message_id: int, size: int, 
                    created_by: str = None, comment: str = None) -> FileVersion:
        """Add a new version of a file"""
        version_id = secrets.token_hex(8)
        version = FileVersion(
            version_id=version_id,
            file_id=file_id,
            message_id=message_id,
            size=size,
            created_at=datetime.now().isoformat(),
            created_by=created_by,
            comment=comment
        )
        self.versions[file_id].append(version)
        logger.info(f"Added version {version_id} for file {file_id}")
        return version
    
    def get_versions(self, file_id: str) -> List[Dict]:
        """Get all versions of a file"""
        return [asdict(v) for v in self.versions.get(file_id, [])]
    
    def get_version(self, file_id: str, version_id: str) -> Optional[FileVersion]:
        """Get specific version"""
        for v in self.versions.get(file_id, []):
            if v.version_id == version_id:
                return v
        return None
    
    def delete_version(self, file_id: str, version_id: str) -> bool:
        """Delete a specific version"""
        if file_id in self.versions:
            self.versions[file_id] = [v for v in self.versions[file_id] if v.version_id != version_id]
            return True
        return False


# ============================================================================
# COLLECTIONS/PLAYLISTS
# ============================================================================

class CollectionManager:
    """Manage file collections and playlists"""
    
    def __init__(self):
        self.collections: Dict[str, Collection] = {}
    
    def create_collection(self, name: str, description: str = "", owner_id: str = "admin",
                          is_public: bool = False, tags: List[str] = None) -> Collection:
        """Create a new collection"""
        collection_id = secrets.token_hex(8)
        collection = Collection(
            id=collection_id,
            name=name,
            description=description,
            owner_id=owner_id,
            is_public=is_public,
            tags=tags or []
        )
        self.collections[collection_id] = collection
        logger.info(f"Created collection: {name}")
        return collection
    
    def add_to_collection(self, collection_id: str, file_path: str) -> bool:
        """Add file to collection"""
        if collection_id in self.collections:
            if file_path not in self.collections[collection_id].file_paths:
                self.collections[collection_id].file_paths.append(file_path)
            return True
        return False
    
    def remove_from_collection(self, collection_id: str, file_path: str) -> bool:
        """Remove file from collection"""
        if collection_id in self.collections:
            if file_path in self.collections[collection_id].file_paths:
                self.collections[collection_id].file_paths.remove(file_path)
            return True
        return False
    
    def get_collection(self, collection_id: str) -> Optional[Dict]:
        """Get collection details"""
        if collection_id in self.collections:
            return asdict(self.collections[collection_id])
        return None
    
    def list_collections(self, owner_id: str = None, include_public: bool = True) -> List[Dict]:
        """List collections"""
        result = []
        for c in self.collections.values():
            if owner_id and c.owner_id != owner_id and not (include_public and c.is_public):
                continue
            result.append(asdict(c))
        return result
    
    def delete_collection(self, collection_id: str) -> bool:
        """Delete collection"""
        if collection_id in self.collections:
            del self.collections[collection_id]
            return True
        return False
    
    def update_collection(self, collection_id: str, **kwargs) -> bool:
        """Update collection properties"""
        if collection_id in self.collections:
            c = self.collections[collection_id]
            for key, value in kwargs.items():
                if hasattr(c, key):
                    setattr(c, key, value)
            return True
        return False


# ============================================================================
# VIDEO CHAPTERS
# ============================================================================

class ChapterManager:
    """Manage video chapters/timestamps"""
    
    def __init__(self):
        self.chapters: Dict[str, List[VideoChapter]] = {}  # file_path -> chapters
    
    def set_chapters(self, file_path: str, chapters: List[Dict]) -> bool:
        """Set chapters for a video"""
        self.chapters[file_path] = [
            VideoChapter(
                title=c["title"],
                start_time=c["start_time"],
                end_time=c.get("end_time"),
                thumbnail=c.get("thumbnail")
            )
            for c in chapters
        ]
        return True
    
    def get_chapters(self, file_path: str) -> List[Dict]:
        """Get chapters for a video"""
        return [asdict(c) for c in self.chapters.get(file_path, [])]
    
    def add_chapter(self, file_path: str, title: str, start_time: float, 
                    end_time: float = None) -> bool:
        """Add a single chapter"""
        if file_path not in self.chapters:
            self.chapters[file_path] = []
        
        chapter = VideoChapter(title=title, start_time=start_time, end_time=end_time)
        self.chapters[file_path].append(chapter)
        # Sort by start time
        self.chapters[file_path].sort(key=lambda x: x.start_time)
        return True
    
    def remove_chapter(self, file_path: str, start_time: float) -> bool:
        """Remove a chapter by start time"""
        if file_path in self.chapters:
            self.chapters[file_path] = [
                c for c in self.chapters[file_path] if c.start_time != start_time
            ]
            return True
        return False
    
    def parse_chapters_from_description(self, description: str) -> List[Dict]:
        """Parse chapters from video description (YouTube-style timestamps)"""
        chapters = []
        # Match patterns like "00:00 Introduction" or "1:23:45 Chapter Title"
        pattern = r'(\d{1,2}:)?(\d{1,2}):(\d{2})\s+(.+)'
        
        for line in description.split('\n'):
            match = re.match(pattern, line.strip())
            if match:
                hours = int(match.group(1)[:-1]) if match.group(1) else 0
                minutes = int(match.group(2))
                seconds = int(match.group(3))
                title = match.group(4).strip()
                
                start_time = hours * 3600 + minutes * 60 + seconds
                chapters.append({
                    "title": title,
                    "start_time": start_time
                })
        
        return chapters


# ============================================================================
# SCHEDULED UPLOADS
# ============================================================================

class ScheduleManager:
    """Manage scheduled uploads"""
    
    def __init__(self):
        self.scheduled_tasks: Dict[str, ScheduledUpload] = {}
        self._running = False
    
    def schedule_upload(self, url: str, destination_path: str, filename: str,
                        scheduled_time: datetime, created_by: str = "admin") -> ScheduledUpload:
        """Schedule a file upload"""
        task_id = secrets.token_hex(8)
        task = ScheduledUpload(
            id=task_id,
            url=url,
            destination_path=destination_path,
            filename=filename,
            scheduled_time=scheduled_time.isoformat(),
            created_by=created_by
        )
        self.scheduled_tasks[task_id] = task
        logger.info(f"Scheduled upload: {filename} at {scheduled_time}")
        return task
    
    def cancel_scheduled(self, task_id: str) -> bool:
        """Cancel a scheduled upload"""
        if task_id in self.scheduled_tasks:
            if self.scheduled_tasks[task_id].status == "pending":
                self.scheduled_tasks[task_id].status = "cancelled"
                return True
        return False
    
    def get_scheduled(self, status: str = None) -> List[Dict]:
        """Get scheduled uploads"""
        tasks = list(self.scheduled_tasks.values())
        if status:
            tasks = [t for t in tasks if t.status == status]
        return [asdict(t) for t in tasks]
    
    def get_due_tasks(self) -> List[ScheduledUpload]:
        """Get tasks that are due for execution"""
        now = datetime.now()
        due = []
        for task in self.scheduled_tasks.values():
            if task.status == "pending":
                scheduled = datetime.fromisoformat(task.scheduled_time)
                if scheduled <= now:
                    due.append(task)
        return due
    
    async def process_scheduled_tasks(self):
        """Background task to process scheduled uploads"""
        self._running = True
        while self._running:
            try:
                due_tasks = self.get_due_tasks()
                for task in due_tasks:
                    task.status = "running"
                    try:
                        # Import here to avoid circular imports
                        from utils.downloader import download_file
                        await download_file(
                            task.url, 
                            task.id, 
                            task.destination_path, 
                            task.filename, 
                            False
                        )
                        task.status = "completed"
                        logger.info(f"Completed scheduled upload: {task.filename}")
                    except Exception as e:
                        task.status = "failed"
                        task.error_message = str(e)
                        logger.error(f"Failed scheduled upload {task.filename}: {e}")
            except Exception as e:
                logger.error(f"Error in scheduled task processor: {e}")
            
            await asyncio.sleep(60)  # Check every minute
    
    def stop(self):
        """Stop the scheduler"""
        self._running = False


# ============================================================================
# FAVORITES & RECENT FILES
# ============================================================================

class FavoritesManager:
    """Manage user favorites and recent files"""
    
    def __init__(self):
        self.favorites: Dict[str, Set[str]] = defaultdict(set)  # user_id -> file_paths
        self.recent: Dict[str, List[Dict]] = defaultdict(list)  # user_id -> [{path, timestamp}]
        self.max_recent = 50
    
    def add_favorite(self, user_id: str, file_path: str) -> bool:
        """Add file to favorites"""
        self.favorites[user_id].add(file_path)
        return True
    
    def remove_favorite(self, user_id: str, file_path: str) -> bool:
        """Remove file from favorites"""
        self.favorites[user_id].discard(file_path)
        return True
    
    def get_favorites(self, user_id: str) -> List[str]:
        """Get user's favorite files"""
        return list(self.favorites.get(user_id, set()))
    
    def is_favorite(self, user_id: str, file_path: str) -> bool:
        """Check if file is favorited"""
        return file_path in self.favorites.get(user_id, set())
    
    def add_recent(self, user_id: str, file_path: str):
        """Add file to recent list"""
        # Remove if already exists
        self.recent[user_id] = [r for r in self.recent[user_id] if r["path"] != file_path]
        # Add to front
        self.recent[user_id].insert(0, {
            "path": file_path,
            "timestamp": datetime.now().isoformat()
        })
        # Trim to max
        self.recent[user_id] = self.recent[user_id][:self.max_recent]
    
    def get_recent(self, user_id: str, limit: int = 20) -> List[Dict]:
        """Get recent files"""
        return self.recent.get(user_id, [])[:limit]
    
    def clear_recent(self, user_id: str):
        """Clear recent files"""
        self.recent[user_id] = []


# ============================================================================
# DUPLICATE DETECTION
# ============================================================================

class DuplicateDetector:
    """Detect duplicate files"""
    
    def __init__(self):
        self.file_hashes: Dict[str, List[str]] = defaultdict(list)  # hash -> file_paths
        self.file_sizes: Dict[int, List[str]] = defaultdict(list)  # size -> file_paths
    
    def register_file(self, file_path: str, size: int, file_hash: str = None):
        """Register a file for duplicate detection"""
        self.file_sizes[size].append(file_path)
        if file_hash:
            self.file_hashes[file_hash].append(file_path)
    
    def find_duplicates_by_size(self, size: int) -> List[str]:
        """Find files with same size"""
        return self.file_sizes.get(size, [])
    
    def find_duplicates_by_hash(self, file_hash: str) -> List[str]:
        """Find files with same hash"""
        return self.file_hashes.get(file_hash, [])
    
    def get_all_duplicates(self) -> List[Dict]:
        """Get all duplicate groups"""
        duplicates = []
        
        # By size (potential duplicates)
        for size, paths in self.file_sizes.items():
            if len(paths) > 1:
                duplicates.append({
                    "type": "size_match",
                    "size": size,
                    "files": paths
                })
        
        # By hash (confirmed duplicates)
        for hash_val, paths in self.file_hashes.items():
            if len(paths) > 1:
                duplicates.append({
                    "type": "hash_match",
                    "hash": hash_val,
                    "files": paths
                })
        
        return duplicates
    
    def remove_file(self, file_path: str, size: int = None, file_hash: str = None):
        """Remove file from tracking"""
        if size and size in self.file_sizes:
            self.file_sizes[size] = [p for p in self.file_sizes[size] if p != file_path]
        if file_hash and file_hash in self.file_hashes:
            self.file_hashes[file_hash] = [p for p in self.file_hashes[file_hash] if p != file_path]


# ============================================================================
# FOLDER PASSWORDS
# ============================================================================

class FolderPasswordManager:
    """Manage folder-level password protection"""

    PERSIST_FILE = Path("backend/folder_passwords.json")

    def __init__(self):
        self.folder_passwords: Dict[str, str] = {}  # folder_path -> password_hash
        self.unlocked_sessions: Dict[str, Set[str]] = defaultdict(set)  # session_token -> unlocked_paths
        self._load()

    def _load(self):
        """Load persisted folder passwords from disk."""
        try:
            if self.PERSIST_FILE.exists():
                with open(self.PERSIST_FILE, "r") as f:
                    self.folder_passwords = json.load(f)
        except Exception:
            self.folder_passwords = {}

    def _save(self):
        """Persist folder passwords to disk."""
        try:
            self.PERSIST_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(self.PERSIST_FILE, "w") as f:
                json.dump(self.folder_passwords, f, indent=2)
        except Exception:
            pass

    def _hash_password(self, password: str) -> str:
        """Hash folder password"""
        return hashlib.sha256(f"folder_{password}".encode()).hexdigest()

    def set_password(self, folder_path: str, password: str) -> bool:
        """Set password for folder"""
        self.folder_passwords[folder_path] = self._hash_password(password)
        self._save()
        return True

    def remove_password(self, folder_path: str) -> bool:
        """Remove password from folder"""
        if folder_path in self.folder_passwords:
            del self.folder_passwords[folder_path]
            self._save()
            return True
        return False
    
    def is_protected(self, folder_path: str) -> bool:
        """Check if folder is password protected"""
        # Check this folder and all parent folders
        parts = folder_path.strip("/").split("/")
        for i in range(len(parts) + 1):
            check_path = "/" + "/".join(parts[:i]) if i > 0 else "/"
            if check_path in self.folder_passwords:
                return True
        return False
    
    def verify_password(self, folder_path: str, password: str, session_token: str = None) -> bool:
        """Verify folder password"""
        # Find the protected folder
        parts = folder_path.strip("/").split("/")
        protected_path = None
        
        for i in range(len(parts), -1, -1):
            check_path = "/" + "/".join(parts[:i]) if i > 0 else "/"
            if check_path in self.folder_passwords:
                protected_path = check_path
                break
        
        if not protected_path:
            return True  # Not protected
        
        if self._hash_password(password) == self.folder_passwords[protected_path]:
            if session_token:
                self.unlocked_sessions[session_token].add(protected_path)
            return True
        return False
    
    def is_unlocked(self, folder_path: str, session_token: str) -> bool:
        """Check if folder is unlocked for session"""
        if not self.is_protected(folder_path):
            return True
        
        parts = folder_path.strip("/").split("/")
        for i in range(len(parts), -1, -1):
            check_path = "/" + "/".join(parts[:i]) if i > 0 else "/"
            if check_path in self.unlocked_sessions.get(session_token, set()):
                return True
        return False


# ============================================================================
# SUBTITLE MANAGER
# ============================================================================

class SubtitleManager:
    """Manage video subtitles"""
    
    def __init__(self):
        self.subtitles: Dict[str, Dict[str, Dict]] = {}  # video_path -> {lang: {file_path, label}}
    
    def add_subtitle(self, video_path: str, language: str, subtitle_path: str, 
                     label: str = None) -> bool:
        """Add subtitle to video"""
        if video_path not in self.subtitles:
            self.subtitles[video_path] = {}
        
        self.subtitles[video_path][language] = {
            "path": subtitle_path,
            "label": label or language.upper()
        }
        return True
    
    def remove_subtitle(self, video_path: str, language: str) -> bool:
        """Remove subtitle"""
        if video_path in self.subtitles and language in self.subtitles[video_path]:
            del self.subtitles[video_path][language]
            return True
        return False
    
    def get_subtitles(self, video_path: str) -> Dict:
        """Get all subtitles for video"""
        return self.subtitles.get(video_path, {})
    
    def has_subtitles(self, video_path: str) -> bool:
        """Check if video has subtitles"""
        return video_path in self.subtitles and len(self.subtitles[video_path]) > 0


# ============================================================================
# QR CODE & SHARING
# ============================================================================

class SharingManager:
    """Advanced sharing features"""
    
    def __init__(self):
        self.share_links: Dict[str, Dict] = {}  # short_code -> {file_path, expires, password, etc}
        self.embed_codes: Dict[str, str] = {}  # file_path -> embed_html
    
    def create_short_link(self, file_path: str, expires_hours: int = 24,
                          password: str = None, max_uses: int = None) -> Dict:
        """Create a short shareable link"""
        short_code = secrets.token_urlsafe(8)
        
        self.share_links[short_code] = {
            "file_path": file_path,
            "created_at": datetime.now().isoformat(),
            "expires_at": (datetime.now() + timedelta(hours=expires_hours)).isoformat() if expires_hours else None,
            "password_hash": hashlib.sha256(password.encode()).hexdigest() if password else None,
            "max_uses": max_uses,
            "use_count": 0
        }
        
        return {
            "short_code": short_code,
            "expires_at": self.share_links[short_code]["expires_at"],
            "password_protected": password is not None
        }
    
    def validate_short_link(self, short_code: str, password: str = None) -> Optional[str]:
        """Validate short link and return file path"""
        if short_code not in self.share_links:
            return None
        
        link = self.share_links[short_code]
        
        # Check expiry
        if link["expires_at"]:
            if datetime.fromisoformat(link["expires_at"]) < datetime.now():
                del self.share_links[short_code]
                return None
        
        # Check max uses
        if link["max_uses"] and link["use_count"] >= link["max_uses"]:
            return None
        
        # Check password
        if link["password_hash"]:
            if not password or hashlib.sha256(password.encode()).hexdigest() != link["password_hash"]:
                return None
        
        link["use_count"] += 1
        return link["file_path"]
    
    def generate_embed_code(self, file_path: str, base_url: str, width: int = 640, 
                            height: int = 360) -> str:
        """Generate embeddable player code"""
        embed_url = f"{base_url}/embed?path={file_path}"
        embed_code = f'''<iframe 
    src="{embed_url}" 
    width="{width}" 
    height="{height}" 
    frameborder="0" 
    allowfullscreen
    allow="autoplay; encrypted-media; picture-in-picture">
</iframe>'''
        self.embed_codes[file_path] = embed_code
        return embed_code
    
    def generate_qr_data(self, url: str) -> str:
        """Generate QR code data URL (returns URL for QR code API)"""
        # Using Google Charts API for QR code generation
        encoded_url = url.replace("&", "%26")
        return f"https://chart.googleapis.com/chart?cht=qr&chs=300x300&chl={encoded_url}"


# ============================================================================
# CACHE MANAGER
# ============================================================================

class CacheManager:
    """Smart caching for frequently accessed files"""
    
    def __init__(self, cache_dir: str = "./cache/files", max_cache_size: int = 5 * 1024 * 1024 * 1024):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_cache_size = max_cache_size  # 5GB default
        self.cache_index: Dict[str, Dict] = {}  # file_id -> {path, size, last_access, access_count}
        self.current_size = 0
    
    def get_cache_path(self, file_id: str) -> Optional[Path]:
        """Get cached file path if exists"""
        if file_id in self.cache_index:
            self.cache_index[file_id]["last_access"] = datetime.now().isoformat()
            self.cache_index[file_id]["access_count"] += 1
            return self.cache_dir / self.cache_index[file_id]["filename"]
        return None
    
    async def cache_file(self, file_id: str, filename: str, data: bytes) -> bool:
        """Cache a file"""
        size = len(data)
        
        # Check if we need to make space
        while self.current_size + size > self.max_cache_size:
            if not self._evict_lru():
                return False  # Can't make enough space
        
        # Save file
        cache_path = self.cache_dir / f"{file_id}_{filename}"
        async with aiofiles.open(cache_path, "wb") as f:
            await f.write(data)
        
        self.cache_index[file_id] = {
            "filename": f"{file_id}_{filename}",
            "size": size,
            "last_access": datetime.now().isoformat(),
            "access_count": 1
        }
        self.current_size += size
        
        return True
    
    def _evict_lru(self) -> bool:
        """Evict least recently used file"""
        if not self.cache_index:
            return False
        
        # Find LRU file
        lru_id = min(self.cache_index.keys(), 
                     key=lambda k: self.cache_index[k]["last_access"])
        
        # Remove it
        info = self.cache_index[lru_id]
        cache_path = self.cache_dir / info["filename"]
        if cache_path.exists():
            cache_path.unlink()
        
        self.current_size -= info["size"]
        del self.cache_index[lru_id]
        
        return True
    
    def clear_cache(self):
        """Clear all cached files"""
        for file_id, info in self.cache_index.items():
            cache_path = self.cache_dir / info["filename"]
            if cache_path.exists():
                cache_path.unlink()
        
        self.cache_index = {}
        self.current_size = 0
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            "total_files": len(self.cache_index),
            "total_size": self.current_size,
            "max_size": self.max_cache_size,
            "usage_percent": (self.current_size / self.max_cache_size) * 100 if self.max_cache_size > 0 else 0
        }


# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

# Initialize all managers
USER_MANAGER = UserManager()
ANALYTICS_MANAGER = AnalyticsManager()
VERSION_MANAGER = VersionManager()
COLLECTION_MANAGER = CollectionManager()
CHAPTER_MANAGER = ChapterManager()
SCHEDULE_MANAGER = ScheduleManager()
FAVORITES_MANAGER = FavoritesManager()
DUPLICATE_DETECTOR = DuplicateDetector()
FOLDER_PASSWORD_MANAGER = FolderPasswordManager()
SUBTITLE_MANAGER = SubtitleManager()
SHARING_MANAGER = SharingManager()
CACHE_MANAGER = CacheManager()

logger.info("Advanced features module initialized")
