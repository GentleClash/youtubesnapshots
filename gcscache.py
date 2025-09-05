from google.cloud import storage
import json
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor
import os 
import logging

# Set up logging for better debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GCSCache:
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.cache_duration = timedelta(hours=24)  # Cache for 24 hours
        # Thread pool for async operations
        self._executor = ThreadPoolExecutor(max_workers=4)
    
    def _get_cache_key(self, video_id: str, timestamp: int, quality: str) -> str:
        """Generate cache key for screenshot"""
        return f"screenshots/{video_id}_{timestamp}_{quality}.png"
    
    def _get_metadata_key(self, video_id: str, timestamp: int) -> str:
        """Generate metadata cache key"""
        return f"metadata/{video_id}_{timestamp}.json"
    
    def get_cached_screenshot(self, video_id: str, timestamp: int, quality: str) -> Optional[bytes]:
        """Retrieve cached screenshot (synchronous)"""
        try:
            cache_key = self._get_cache_key(video_id, timestamp, quality)
            logger.info(f"GCS lookup: {cache_key}")
            
            blob = self.bucket.blob(cache_key)
            
            # Use blob.exists() with retry logic
            try:
                exists = blob.exists()
            except Exception as e:
                logger.warning(f"Error checking blob existence for {cache_key}: {e}")
                return None
                
            if not exists:
                logger.info(f"GCS blob does not exist: {cache_key}")
                return None
            
            try:
                # Only reload if we need fresh metadata
                if blob.time_created is None:
                    blob.reload()
                
                # Check expiration
                if blob.time_created:
                    # Use UTC timezone for consistency
                    current_time = datetime.now(timezone.utc)
                    blob_time = blob.time_created.replace(tzinfo=timezone.utc) if blob.time_created.tzinfo is None else blob.time_created
                    age = current_time - blob_time
                    
                    if age >= self.cache_duration:
                        logger.info(f"GCS cache EXPIRED: {cache_key} (age: {age})")
                        return None
                    
                    logger.info(f"GCS cache HIT: {cache_key} (age: {age})")
                else:
                    logger.info(f"GCS cache HIT (no timestamp): {cache_key}")
                
                # Download the data
                data = blob.download_as_bytes()
                logger.info(f"GCS downloaded {len(data)} bytes for {cache_key}")
                return data
                
            except Exception as e:
                logger.error(f"Error processing blob metadata for {cache_key}: {e}")
                # Try to download anyway if metadata fails
                try:
                    data = blob.download_as_bytes()
                    logger.info(f"GCS downloaded {len(data)} bytes for {cache_key} (fallback)")
                    return data
                except Exception as download_error:
                    logger.error(f"Fallback download failed for {cache_key}: {download_error}")
                    return None
                
        except Exception as e:
            logger.error(f"GCS cache retrieval error for {cache_key}: {e}")
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
        """Cache screenshot to GCS (synchronous)"""
        try:
            cache_key = self._get_cache_key(video_id, timestamp, quality)
            blob = self.bucket.blob(cache_key)
            blob.upload_from_string(image_data, content_type='image/png')
            logger.info(f"Cached screenshot: {blob.name} ({len(image_data)} bytes)")
        except Exception as e:
            logger.error(f"Cache storage error: {e}")
    
    async def cache_screenshot_async(self, video_id: str, timestamp: int, quality: str, image_data: bytes):
        """Cache screenshot to GCS (asynchronous)"""
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
            metadata_key = self._get_metadata_key(video_id, timestamp)
            blob = self.bucket.blob(metadata_key)
            
            if not blob.exists():
                logger.info(f"Metadata blob does not exist: {metadata_key}")
                return None
            
            # Check if cache is still valid
            try:
                if blob.time_created is None:
                    blob.reload()
                
                if blob.time_created:
                    current_time = datetime.now(timezone.utc)
                    blob_time = blob.time_created.replace(tzinfo=timezone.utc) if blob.time_created.tzinfo is None else blob.time_created
                    age = current_time - blob_time
                    
                    if age >= self.cache_duration:
                        logger.info(f"Metadata cache EXPIRED: {metadata_key} (age: {age})")
                        return None
                
                data = json.loads(blob.download_as_text())
                logger.info(f"Metadata cache HIT: {metadata_key}")
                return data
                
            except Exception as e:
                logger.error(f"Error processing metadata blob {metadata_key}: {e}")
                return None
            
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
            metadata_key = self._get_metadata_key(video_id, timestamp)
            blob = self.bucket.blob(metadata_key)
            blob.upload_from_string(json.dumps(metadata), content_type='application/json')
            logger.info(f"Cached metadata: {blob.name}")
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
            
            # List all blobs in screenshots folder
            screenshot_blobs = self.bucket.list_blobs(prefix="screenshots/")
            for blob in screenshot_blobs:
                if blob.time_created:
                    blob_time = blob.time_created.replace(tzinfo=timezone.utc) if blob.time_created.tzinfo is None else blob.time_created
                    if blob_time < cutoff_time:
                        blob.delete()
                        deleted_count += 1
            
            # List all blobs in metadata folder
            metadata_blobs = self.bucket.list_blobs(prefix="metadata/")
            for blob in metadata_blobs:
                if blob.time_created:
                    blob_time = blob.time_created.replace(tzinfo=timezone.utc) if blob.time_created.tzinfo is None else blob.time_created
                    if blob_time < cutoff_time:
                        blob.delete()
                        deleted_count += 1
            
            logger.info(f"Cleaned up {deleted_count} expired cache entries")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Cache cleanup error: {e}")
            return 0
    
    async def cleanup_expired_cache_async(self) -> int:
        """Clean up expired cache entries (asynchronous)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.cleanup_expired_cache)
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        try:
            screenshot_blobs = list(self.bucket.list_blobs(prefix="screenshots/"))
            metadata_blobs = list(self.bucket.list_blobs(prefix="metadata/"))
            
            # Calculate total size
            total_size = sum(blob.size or 0 for blob in screenshot_blobs + metadata_blobs)
            
            # Count by quality
            quality_counts = {}
            for blob in screenshot_blobs:
                parts = blob.name.split('_')
                if len(parts) >= 3:
                    quality = parts[-1].split('.')[0]  # Remove .png extension
                    quality_counts[quality] = quality_counts.get(quality, 0) + 1
            
            return {
                "screenshot_count": len(screenshot_blobs),
                "metadata_count": len(metadata_blobs),
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "quality_breakdown": quality_counts,
                "cache_duration_hours": self.cache_duration.total_seconds() / 3600
            }
            
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {"error": str(e)}

    def __del__(self) -> None:
        """Cleanup thread pool on destruction"""
        if hasattr(self, '_executor'):
            self._executor.shutdown(wait=False)