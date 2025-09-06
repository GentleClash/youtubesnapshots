import json
import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor
import logging

# Set up logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalCache:
    def __init__(self, cache_dir: str = "cache"):
        self.cache_dir = cache_dir
        self.cache_duration = timedelta(hours=24)  # Cache for 24 hours
        # Thread pool for async operations
        self._executor = ThreadPoolExecutor(max_workers=4)
        
        # Create cache directories
        self.screenshots_dir = os.path.join(cache_dir, "screenshots")
        self.metadata_dir = os.path.join(cache_dir, "metadata")
        os.makedirs(self.screenshots_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)
    
    def _get_cache_path(self, video_id: str, timestamp: int, quality: str) -> str:
        """Generate cache file path for screenshot"""
        filename = f"{video_id}_{timestamp}_{quality}.png"
        return os.path.join(self.screenshots_dir, filename)
    
    def _get_metadata_path(self, video_id: str, timestamp: int) -> str:
        """Generate metadata cache file path"""
        filename = f"{video_id}_{timestamp}.json"
        return os.path.join(self.metadata_dir, filename)
    
    def _is_file_expired(self, filepath: str) -> bool:
        """Check if file is expired based on modification time"""
        try:
            if not os.path.exists(filepath):
                return True
            
            file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=timezone.utc)
            current_time = datetime.now(timezone.utc)
            age = current_time - file_mtime
            
            return age >= self.cache_duration
        except Exception as e:
            logger.error(f"Error checking file expiration for {filepath}: {e}")
            return True
    
    def get_cached_screenshot(self, video_id: str, timestamp: int, quality: str) -> Optional[bytes]:
        """Retrieve cached screenshot (synchronous)"""
        try:
            cache_path = self._get_cache_path(video_id, timestamp, quality)
            logger.info(f"Local cache lookup: {cache_path}")
            
            if not os.path.exists(cache_path):
                logger.info(f"Local cache file does not exist: {cache_path}")
                return None
            
            # Check expiration
            if self._is_file_expired(cache_path):
                logger.info(f"Local cache EXPIRED: {cache_path}")
                # Optionally remove expired file
                try:
                    os.remove(cache_path)
                except Exception as e:
                    logger.warning(f"Failed to remove expired cache file {cache_path}: {e}")
                return None
            
            # Read the file
            with open(cache_path, 'rb') as f:
                data = f.read()
            
            file_age = datetime.now(timezone.utc) - datetime.fromtimestamp(os.path.getmtime(cache_path), tz=timezone.utc)
            logger.info(f"Local cache HIT: {cache_path} (age: {file_age}, size: {len(data)} bytes)")
            return data
                
        except Exception as e:
            logger.error(f"Local cache retrieval error for {cache_path}: {e}")
            return None
    
    async def get_cached_screenshot_async(self, video_id: str, timestamp: int, quality: str) -> Optional[bytes]:
        """Retrieve cached screenshot (asynchronous)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, 
            self.get_cached_screenshot, 
            video_id, 
            timestamp, 
            quality
        )
    
    def cache_screenshot(self, video_id: str, timestamp: int, quality: str, image_data: bytes):
        """Cache screenshot to local filesystem (synchronous)"""
        try:
            cache_path = self._get_cache_path(video_id, timestamp, quality)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            
            # Write the file
            with open(cache_path, 'wb') as f:
                f.write(image_data)
            
            logger.info(f"Cached screenshot: {cache_path} ({len(image_data)} bytes)")
        except Exception as e:
            logger.error(f"Local cache storage error: {e}")
    
    async def cache_screenshot_async(self, video_id: str, timestamp: int, quality: str, image_data: bytes):
        """Cache screenshot to local filesystem (asynchronous)"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self._executor,
            self.cache_screenshot,
            video_id,
            timestamp,
            quality,
            image_data
        )
    
    def get_cached_metadata(self, video_id: str, timestamp: int) -> Optional[List[Dict]]:
        """Get cached screenshot metadata (synchronous)"""
        try:
            metadata_path = self._get_metadata_path(video_id, timestamp)
            logger.info(f"Local metadata lookup: {metadata_path}")
            
            if not os.path.exists(metadata_path):
                logger.info(f"Metadata file does not exist: {metadata_path}")
                return None
            
            # Check if cache is still valid
            if self._is_file_expired(metadata_path):
                logger.info(f"Metadata cache EXPIRED: {metadata_path}")
                # Optionally remove expired file
                try:
                    os.remove(metadata_path)
                except Exception as e:
                    logger.warning(f"Failed to remove expired metadata file {metadata_path}: {e}")
                return None
            
            # Read and parse JSON
            with open(metadata_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"Metadata cache HIT: {metadata_path}")
            return data
                
        except Exception as e:
            logger.error(f"Metadata cache retrieval error: {e}")
            return None
    
    async def get_cached_metadata_async(self, video_id: str, timestamp: int) -> Optional[List[Dict]]:
        """Get cached screenshot metadata (asynchronous)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self.get_cached_metadata,
            video_id,
            timestamp
        )
    
    def cache_metadata(self, video_id: str, timestamp: int, metadata: List[Dict]):
        """Cache screenshot metadata (synchronous)"""
        try:
            metadata_path = self._get_metadata_path(video_id, timestamp)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
            
            # Write JSON file
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Cached metadata: {metadata_path}")
        except Exception as e:
            logger.error(f"Metadata cache error: {e}")
    
    async def cache_metadata_async(self, video_id: str, timestamp: int, metadata: List[Dict]):
        """Cache screenshot metadata (asynchronous)"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            self._executor,
            self.cache_metadata,
            video_id,
            timestamp,
            metadata
        )
    
    async def batch_check_cached_screenshots(self, video_id: str, timestamp: int, qualities: List[str]) -> Dict[str, Optional[bytes]]:
        """Check multiple screenshot qualities in parallel"""
        tasks = [
            self.get_cached_screenshot_async(video_id, timestamp, quality)
            for quality in qualities
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {
            quality: result if not isinstance(result, Exception) else None
            for quality, result in zip(qualities, results)
        } #type: ignore
    
    async def batch_cache_screenshots(self, video_id: str, timestamp: int, screenshots: List[Dict]):
        """Cache multiple screenshots in parallel"""
        cache_tasks = []
        
        for screenshot in screenshots:
            if 'filename' in screenshot and os.path.exists(screenshot['filename']):
                try:
                    with open(screenshot['filename'], 'rb') as f:
                        image_data = f.read()
                        task = self.cache_screenshot_async(
                            video_id, 
                            timestamp, 
                            screenshot['quality'], 
                            image_data
                        )
                        cache_tasks.append(task)
                except Exception as e:
                    logger.error(f"Error reading screenshot file {screenshot['filename']}: {e}")
        
        if cache_tasks:
            await asyncio.gather(*cache_tasks, return_exceptions=True)
    
    def cleanup_expired_cache(self) -> int:
        """Clean up expired cache entries"""
        try:
            deleted_count = 0
            cutoff_time = datetime.now(timezone.utc) - self.cache_duration
            
            # Clean up screenshot files
            for filename in os.listdir(self.screenshots_dir):
                filepath = os.path.join(self.screenshots_dir, filename)
                try:
                    if os.path.isfile(filepath):
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=timezone.utc)
                        if file_mtime < cutoff_time:
                            os.remove(filepath)
                            deleted_count += 1
                except Exception as e:
                    logger.warning(f"Error processing file {filepath}: {e}")
            
            # Clean up metadata files
            for filename in os.listdir(self.metadata_dir):
                filepath = os.path.join(self.metadata_dir, filename)
                try:
                    if os.path.isfile(filepath):
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath), tz=timezone.utc)
                        if file_mtime < cutoff_time:
                            os.remove(filepath)
                            deleted_count += 1
                except Exception as e:
                    logger.warning(f"Error processing file {filepath}: {e}")
            
            logger.info(f"Cleaned up {deleted_count} expired local cache entries")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Local cache cleanup error: {e}")
            return 0
    
    async def cleanup_expired_cache_async(self) -> int:
        """Clean up expired cache entries (asynchronous)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.cleanup_expired_cache)
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        try:
            screenshot_files = []
            metadata_files = []
            
            # Count screenshot files and calculate sizes
            total_size = 0
            quality_counts = {}
            
            for filename in os.listdir(self.screenshots_dir):
                filepath = os.path.join(self.screenshots_dir, filename)
                if os.path.isfile(filepath):
                    screenshot_files.append(filename)
                    total_size += os.path.getsize(filepath)
                    
                    # Extract quality from filename
                    parts = filename.split('_')
                    if len(parts) >= 3:
                        quality = parts[-1].split('.')[0]  # Remove .png extension
                        quality_counts[quality] = quality_counts.get(quality, 0) + 1
            
            # Count metadata files
            for filename in os.listdir(self.metadata_dir):
                filepath = os.path.join(self.metadata_dir, filename)
                if os.path.isfile(filepath):
                    metadata_files.append(filename)
                    total_size += os.path.getsize(filepath)
            
            return {
                "screenshot_count": len(screenshot_files),
                "metadata_count": len(metadata_files),
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "quality_breakdown": quality_counts,
                "cache_duration_hours": self.cache_duration.total_seconds() / 3600,
                "cache_directory": self.cache_dir
            }
            
        except Exception as e:
            logger.error(f"Error getting local cache stats: {e}")
            return {"error": str(e)}
    
    def clear_all_cache(self) -> int:
        """Clear all cache files (useful for testing)"""
        try:
            deleted_count = 0
            
            # Clear screenshot files
            for filename in os.listdir(self.screenshots_dir):
                filepath = os.path.join(self.screenshots_dir, filename)
                try:
                    if os.path.isfile(filepath):
                        os.remove(filepath)
                        deleted_count += 1
                except Exception as e:
                    logger.warning(f"Error removing file {filepath}: {e}")
            
            # Clear metadata files
            for filename in os.listdir(self.metadata_dir):
                filepath = os.path.join(self.metadata_dir, filename)
                try:
                    if os.path.isfile(filepath):
                        os.remove(filepath)
                        deleted_count += 1
                except Exception as e:
                    logger.warning(f"Error removing file {filepath}: {e}")
            
            logger.info(f"Cleared all local cache: {deleted_count} files removed")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error clearing local cache: {e}")
            return 0

    def __del__(self) -> None:
        """Cleanup thread pool on destruction"""
        if hasattr(self, '_executor'):
            self._executor.shutdown(wait=False)