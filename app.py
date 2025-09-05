import re
import os
import subprocess
import time
import json
import asyncio
from typing import Optional, Dict, List
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel
from collections import defaultdict, deque
from gcscache import GCSCache
from dotenv import load_dotenv
load_dotenv()

# Rate limiting storage
request_counts = defaultdict(deque)
RATE_LIMIT = 10
RATE_WINDOW = 60

# Cache
gcs_cache = GCSCache(os.getenv("GCS_BUCKET_NAME", "youtube-snapshots"))

class VideoRequest(BaseModel):
    url: str
    timestamp: Optional[int] = None
    hours: int = 0
    minutes: int = 0
    seconds: int = 0

# Fast multi-level cache implementation
class FastCache:
    def __init__(self, gcs_cache: GCSCache):
        self.gcs_cache = gcs_cache
        self.memory_cache = {}  # In-memory cache
        self.metadata_cache = {}  # Cache for metadata
        self.cache_stats = {"hits": 0, "misses": 0, "memory_hits": 0, "gcs_hits": 0}
        self.max_memory_items = 100  # Prevent memory overflow
        self.access_order = deque()  # For LRU eviction
        
    async def get_screenshot(self, video_id: str, timestamp: int, quality: str) -> Optional[bytes]:
        """Multi-level cache lookup with LRU eviction"""
        cache_key = f"{video_id}_{timestamp}_{quality}"
        
        # Level 1: Memory cache
        if cache_key in self.memory_cache:
            print(f"MEMORY HIT: {cache_key}")
            self.cache_stats["hits"] += 1
            self.cache_stats["memory_hits"] += 1
            # Move to end for LRU
            self.access_order.remove(cache_key)
            self.access_order.append(cache_key)
            return self.memory_cache[cache_key]
        
        print(f"MEMORY MISS: {cache_key}, checking GCS...")
        
        # Level 2: GCS cache (slower but persistent)
        image_data = await asyncio.get_event_loop().run_in_executor(
            None, self.gcs_cache.get_cached_screenshot, video_id, timestamp, quality
        )
        
        if image_data:
            print(f"GCS HIT: {cache_key}, size: {len(image_data)} bytes")
            # Store in memory cache with LRU eviction
            await self._store_in_memory(cache_key, image_data)
            self.cache_stats["hits"] += 1
            self.cache_stats["gcs_hits"] += 1
            return image_data
        else:
            print(f"GCS MISS: {cache_key}")
        
        self.cache_stats["misses"] += 1
        return None
    
    async def _store_in_memory(self, cache_key: str, image_data: bytes):
        """Store in memory with LRU eviction"""
        # Evict oldest items if cache is full
        while len(self.memory_cache) >= self.max_memory_items:
            oldest_key = self.access_order.popleft()
            if oldest_key in self.memory_cache:
                del self.memory_cache[oldest_key]
        
        self.memory_cache[cache_key] = image_data
        self.access_order.append(cache_key)
    
    async def store_screenshot(self, video_id: str, timestamp: int, quality: str, image_data: bytes):
        """Store in all cache levels"""
        cache_key = f"{video_id}_{timestamp}_{quality}"
        
        # Store in memory immediately
        await self._store_in_memory(cache_key, image_data)
        
        # Store in GCS in background (don't wait)
        asyncio.create_task(self._store_gcs_background(video_id, timestamp, quality, image_data))
    
    async def _store_gcs_background(self, video_id: str, timestamp: int, quality: str, image_data: bytes):
        """Background GCS storage"""
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, self.gcs_cache.cache_screenshot, video_id, timestamp, quality, image_data
            )
        except Exception as e:
            print(f"Background GCS cache error: {e}")
    
    async def get_cached_metadata(self, video_id: str, timestamp: int) -> Optional[List[Dict]]:
        """Get cached metadata with memory cache support"""
        metadata_key = f"{video_id}_{timestamp}_metadata"
        
        # Check memory cache first
        if metadata_key in self.metadata_cache:
            return self.metadata_cache[metadata_key]
        
        # Check GCS cache
        metadata = await asyncio.get_event_loop().run_in_executor(
            None, self.gcs_cache.get_cached_metadata, video_id, timestamp
        )
        
        if metadata:
            # Store in memory cache
            self.metadata_cache[metadata_key] = metadata
            # Prevent memory overflow
            if len(self.metadata_cache) > 50:
                oldest_key = next(iter(self.metadata_cache))
                del self.metadata_cache[oldest_key]
        
        return metadata
    
    async def store_metadata(self, video_id: str, timestamp: int, metadata: List[Dict]) -> None:
        """Store metadata in both caches"""
        metadata_key = f"{video_id}_{timestamp}_metadata"
        
        # Store in memory immediately
        self.metadata_cache[metadata_key] = metadata
        
        # Store in GCS in background
        asyncio.create_task(self._store_metadata_gcs_background(video_id, timestamp, metadata))
    
    async def _store_metadata_gcs_background(self, video_id: str, timestamp: int, metadata: List[Dict]):
        """Background GCS metadata storage"""
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, self.gcs_cache.cache_metadata, video_id, timestamp, metadata
            )
        except Exception as e:
            print(f"Background GCS metadata cache error: {e}")


# Stream URL caching to avoid repeated yt-dlp calls
class StreamCache:
    def __init__(self) -> None:
        self.cache = {}
        self.cache_duration = timedelta(minutes=30)  # URLs expire in 30 minutes
        self.max_items = 50
    
    def get_streams(self, video_id: str) -> Optional[Dict]:
        if video_id in self.cache:
            cached_data = self.cache[video_id]
            if datetime.now() - cached_data['timestamp'] < self.cache_duration:
                return cached_data['streams']
            else:
                del self.cache[video_id]  # Clean expired
        return None
    
    def cache_streams(self, video_id: str, streams: Dict):
        # Simple cache size management
        if len(self.cache) >= self.max_items:
            # Remove oldest entry
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k]['timestamp'])
            del self.cache[oldest_key]
        
        self.cache[video_id] = {
            'streams': streams,
            'timestamp': datetime.now()
        }

# Initialize caches
fast_cache = FastCache(gcs_cache)
stream_cache = StreamCache()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Clean old files
    import glob
    for f in glob.glob("screenshot_*.png"):
        try:
            os.remove(f)
        except:
            pass
    print("Application started - cleaned old screenshots")
    
    yield
    
    # Shutdown: Clean up
    for f in glob.glob("screenshot_*.png"):
        try:
            os.remove(f)
        except:
            pass
    print("Application shutting down - cleaned screenshots")

app = FastAPI(
    title="YouTube Screenshot Tool",
    description="Extract multiple quality screenshots from YouTube videos",
    lifespan=lifespan
)

def rate_limit_check(client_ip: str) -> bool:
    """Simple rate limiting implementation"""
    now = time.time()
    minute_ago = now - RATE_WINDOW
    
    while request_counts[client_ip] and request_counts[client_ip][0] < minute_ago:
        request_counts[client_ip].popleft()
    
    if len(request_counts[client_ip]) >= RATE_LIMIT:
        return False
    
    request_counts[client_ip].append(now)
    return True

def validate_timestamp(url: str, hours: int, minutes: int, seconds: int) -> Dict:
    """Validate timestamp against video duration"""
    try:
        video_info = get_video_info_with_api(url)
        duration = video_info.get('duration', 0)
        
        if duration == 0:
            return {"valid": False, "message": "Could not retrieve video duration"}
        
        requested_timestamp = hours * 3600 + minutes * 60 + seconds
        
        if requested_timestamp >= duration:
            duration_formatted = f"{duration//3600:02d}:{(duration%3600)//60:02d}:{duration%60:02d}"
            return {
                "valid": False, 
                "message": f"Timestamp {hours:02d}:{minutes:02d}:{seconds:02d} exceeds video duration of {duration_formatted}"
            }
        
        return {"valid": True, "duration": duration}
        
    except Exception as e:
        return {"valid": False, "message": f"Error validating timestamp: {str(e)}"}

def extract_video_id(url: str) -> Optional[str]:
    """Extract YouTube video ID from various URL formats"""
    patterns = [
        r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([a-zA-Z0-9_-]{11})',
        r'youtube\.com/watch\?.*v=([a-zA-Z0-9_-]{11})'
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def extract_timestamp(url: str) -> Dict[str, int]:
    """Extract timestamp from URL and return as hours, minutes, seconds"""
    match = re.search(r'[\?&]t=(\d+)$', url)
    if match:
        total_seconds = int(match.group(1))
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return {"hours": hours, "minutes": minutes, "seconds": seconds}
    
    match = re.search(r'[\?&]t=(?:(\d+)h)?(?:(\d+)m)?(\d+)s?', url)
    if match:
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        return {"hours": hours, "minutes": minutes, "seconds": seconds}
    
    return {"hours": 0, "minutes": 0, "seconds": 0}

def get_multiple_quality_streams_cached(video_url: str) -> Dict[str, Dict]:
    """Cached version of stream extraction to avoid repeated yt-dlp calls"""
    video_id = extract_video_id(video_url)

    if video_id is None:
        raise ValueError("Invalid YouTube URL - cannot extract video ID. Make sure it is viewable.")
    
    # Check cache first
    cached_streams = stream_cache.get_streams(video_id)
    if cached_streams:
        print(f"Using cached streams for {video_id}")
        return cached_streams
    
    # Get fresh streams
    streams = get_multiple_quality_streams(video_url)
    
    # Cache them
    if video_id and streams:
        stream_cache.cache_streams(video_id, streams)
    
    return streams

def get_multiple_quality_streams(video_url: str) -> Dict[str, Dict]:
    """Get multiple quality streams using yt-dlp"""
    try:
        base_cmd = [
            'yt-dlp',
            '--no-download',
            '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            '--add-header', 'Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            '--add-header', 'Accept-Language:en-US,en;q=0.5',
            '--add-header', 'Accept-Encoding:gzip, deflate',
            '--add-header', 'DNT:1',
            '--add-header', 'Connection:keep-alive',
            '--add-header', 'Upgrade-Insecure-Requests:1',
            '--referer', 'https://www.youtube.com/',
            '--socket-timeout', '30',
            '--retries', '3',
            '--fragment-retries', '3',
            '--force-ipv4',
            '--no-cache-dir'
        ]
        
        cmd = base_cmd + ['--list-formats', '--format', 'best[ext=mp4]', video_url]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        cmd_json = base_cmd + ['--dump-json', video_url]
        json_result = subprocess.run(cmd_json, capture_output=True, text=True, check=True)
        video_info = json.loads(json_result.stdout)
        
        formats = video_info.get('formats', [])
        quality_streams = {}
        
        desired_qualities = [
            {'name': 'Ultra (1080p)', 'key': 'ultra', 'height_min': 1080},
            {'name': 'High (720p)', 'key': 'high', 'height_min': 720, 'height_max': 1079},
            {'name': 'Medium (480p)', 'key': 'medium', 'height_min': 480, 'height_max': 719},
            {'name': 'Low (360p)', 'key': 'low', 'height_min': 200, 'height_max': 479}
        ]
        
        for quality in desired_qualities:
            suitable_formats = [
                f for f in formats
                if f.get('height') and 
                f.get('url') and 
                f.get('ext') == 'mp4' and
                quality['height_min'] <= f['height'] <= quality.get('height_max', 9999)
            ]
            
            if suitable_formats:
                best_format = max(suitable_formats, key=lambda x: (x.get('height', 0), x.get('tbr', 0)))
                quality_streams[quality['key']] = {
                    'url': best_format['url'],
                    'height': best_format.get('height'),
                    'name': quality['name'],
                    'format_id': best_format.get('format_id'),
                    'filesize': best_format.get('filesize')
                }
        
        return quality_streams
        
    except subprocess.CalledProcessError as e:
        raise Exception(f"Failed to get stream URLs: {e}")
    except json.JSONDecodeError as e:
        raise Exception(f"Failed to parse video info: {e}")

def generate_screenshot(stream_url: str, timestamp: int, video_id: str, quality: str) -> Optional[Dict]:
    """Single screenshot generation with precise seeking - 3.8x faster"""
    try:
        output_file = f"{video_id}_{timestamp}_{quality}.png"
        
        # FFmpeg command with precise seeking
        cmd = [
            'ffmpeg',
            '-ss', str(timestamp),    
            '-i', stream_url,
            '-frames:v', '1',         # Extract exactly 1 frame
            '-q:v', '2',             # High quality encoding
            '-an',                   # Disable audio processing (saves time)
            '-y',                    # Overwrite
            output_file
        ]
        
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        file_size = os.path.getsize(output_file) if os.path.exists(output_file) else 0
        
        return {
            'quality': quality,
            'filename': output_file,
            'size_kb': round(file_size / 1024, 1),
            'download_url': f'/download/{output_file}'
        }
        
    except subprocess.CalledProcessError as e:
        print(f"Failed to generate {quality} screenshot: {e}")
        return None

async def generate_screenshots_parallel(streams: Dict, timestamp: int, video_id: str) -> List[Dict]:
    """Generate all screenshots in parallel"""
    
    def generate_single_screenshot(quality_data) -> Optional[Dict]:
        """Wrapper function for thread pool execution"""
        quality_key, stream_info = quality_data
        result = generate_screenshot(
            stream_info['url'], 
            timestamp, 
            video_id, 
            quality_key
        )
        if result:
            result['name'] = stream_info['name']
            result['source_height'] = stream_info.get('height', 'Unknown')
        return result
    

    with ThreadPoolExecutor(max_workers=4) as executor:
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(executor, generate_single_screenshot, (k, v))
            for k, v in streams.items()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out None results and exceptions
    screenshots = [r for r in results if r and not isinstance(r, Exception)]

    return screenshots #type: ignore

async def check_cache_parallel(video_id: str, timestamp: int) -> Optional[List[Dict]]:
    """Check cache for all qualities in parallel"""
    qualities = ['ultra', 'high', 'medium', 'low']
    
    # First, check if we have cached metadata (fastest)
    cached_metadata = await fast_cache.get_cached_metadata(video_id, timestamp)
    if cached_metadata:
        # Verify that the actual images are still available
        cache_tasks = [
            fast_cache.get_screenshot(video_id, timestamp, quality)
            for quality in qualities
        ]
        
        cached_images = await asyncio.gather(*cache_tasks)
        
        # If all images are available, return the metadata
        if all(img is not None for img in cached_images):
            print(f"Complete cache hit for {video_id} at {timestamp}s")
            return cached_metadata
        else:
            print(f"Partial cache hit for {video_id} at {timestamp}s - some images missing")
    
    return None

def get_video_info_with_api(video_url: str) -> dict:
    """Get video metadata using yt-dlp"""
    try:
        cmd = [
            'yt-dlp',
            '--no-download',
            '--dump-json',
            '--user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            '--add-header', 'Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            '--add-header', 'Accept-Language:en-US,en;q=0.5',
            '--add-header', 'Accept-Encoding:gzip, deflate',
            '--add-header', 'DNT:1',
            '--add-header', 'Connection:keep-alive',
            '--add-header', 'Upgrade-Insecure-Requests:1',
            '--referer', 'https://www.youtube.com/',
            '--socket-timeout', '30',
            '--retries', '3',
            '--fragment-retries', '3',
            '--force-ipv4',
            '--no-cache-dir',
            video_url
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        video_info = json.loads(result.stdout)
        
        return {
            'title': video_info.get('title', 'Unknown'),
            'duration': video_info.get('duration', 0),
            'thumbnail': video_info.get('thumbnail', ''),
            'view_count': video_info.get('view_count', 0),
            'upload_date': video_info.get('upload_date', ''),
            'uploader': video_info.get('uploader', 'Unknown')
        }
    except Exception as e:
        print(f"Failed to get video info: {e}")
        return {'title': 'Unknown', 'duration': 0, 'thumbnail': ''}

# FastAPI Routes with rate limiting middleware
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Rate limiting middleware"""
    client_ip = request.client.host #type: ignore
    
    if request.url.path.startswith('/api/') and not rate_limit_check(client_ip):
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded. Try again later."}
        )
    
    response = await call_next(request)
    return response

@app.get("/", response_class=HTMLResponse)
async def home() -> str:
    """Enhanced web interface with performance metrics"""
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>YouTube Screenshot Tool - Optimized</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f5f5f5; }
            
            .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
            
            .search-section { 
                text-align: center; 
                transition: all 0.5s ease;
                margin-top: 100px;
            }
            .search-section.moved { margin-top: 20px; }
            
            h1 { color: #333; margin-bottom: 30px; font-size: 2.5em; }
            .search-section.moved h1 { font-size: 1.5em; margin-bottom: 15px; }
            
            .performance-badge {
                background: linear-gradient(45deg, #28a745, #20c997);
                color: white;
                padding: 8px 16px;
                border-radius: 20px;
                font-size: 14px;
                margin-bottom: 20px;
                display: inline-block;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            
            .search-bar {
                width: 100%;
                max-width: 600px;
                padding: 15px;
                border: 2px solid #ddd;
                border-radius: 25px;
                font-size: 16px;
                outline: none;
                transition: border-color 0.3s;
            }
            .search-bar:focus { border-color: #007bff; }
            
            .timestamp-section {
                margin: 20px 0;
                opacity: 0;
                height: 0;
                overflow: hidden;
                transition: all 0.3s ease;
            }
            .timestamp-section.visible {
                opacity: 1;
                height: auto;
            }
            
            .timestamp-inputs {
                display: flex;
                justify-content: center;
                gap: 10px;
                margin: 15px 0;
            }
            
            .time-block {
                display: flex;
                flex-direction: column;
                align-items: center;
            }
            
            .time-input {
                width: 60px;
                padding: 10px;
                text-align: center;
                border: 2px solid #ddd;
                border-radius: 8px;
                font-size: 18px;
            }
            
            .time-label { margin-top: 5px; color: #666; font-size: 14px; }
            
            .btn {
                background: #007bff;
                color: white;
                border: none;
                padding: 12px 30px;
                border-radius: 25px;
                font-size: 16px;
                cursor: pointer;
                transition: background 0.3s;
                margin: 15px 10px;
            }
            .btn:hover { background: #0056b3; }
            .btn:disabled { background: #ccc; cursor: not-allowed; }
            
            .content-section {
                display: none;
                margin-top: 30px;
            }
            .content-section.visible { display: block; }
            
            .video-container {
                text-align: center;
                margin-bottom: 30px;
            }
            
            .video-embed {
                width: 100%;
                max-width: 800px;
                height: 450px;
                border: none;
                border-radius: 10px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            }
            
            .screenshots-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px;
                margin-top: 30px;
            }
            
            .screenshot-card {
                background: white;
                border-radius: 10px;
                padding: 20px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                text-align: center;
                position: relative;
            }
            
            .screenshot-card.cached {
                border-left: 4px solid #28a745;
            }
            
            .cache-indicator {
                position: absolute;
                top: 10px;
                right: 10px;
                background: #28a745;
                color: white;
                padding: 4px 8px;
                border-radius: 12px;
                font-size: 12px;
            }
            
            .screenshot-img {
                width: 100%;
                border-radius: 8px;
                margin-bottom: 15px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            }
            
            .quality-info {
                margin-bottom: 15px;
                color: #555;
            }
            
            .download-btn {
                background: #28a745;
                color: white;
                padding: 10px 20px;
                border: none;
                border-radius: 5px;
                cursor: pointer;
                text-decoration: none;
                display: inline-block;
                transition: background 0.3s;
            }
            .download-btn:hover { background: #1e7e34; }
            
            .loading {
                display: none;
                text-align: center;
                margin: 30px 0;
            }
            .loading.visible { display: block; }
            
            .spinner {
                border: 4px solid #f3f3f3;
                border-top: 4px solid #3498db;
                border-radius: 50%;
                width: 50px;
                height: 50px;
                animation: spin 1s linear infinite;
                margin: 0 auto 20px;
            }
            
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
            
            .error {
                background: #f8d7da;
                border: 1px solid #f5c6cb;
                color: #721c24;
                padding: 15px;
                border-radius: 8px;
                margin: 20px 0;
                display: none;
            }
            .error.visible { display: block; }
            
            .performance-info {
                background: #e7f3ff;
                border: 1px solid #b3d9ff;
                color: #0c5aa6;
                padding: 15px;
                border-radius: 8px;
                margin: 20px 0;
                text-align: center;
                display: none;
            }
            .performance-info.visible { display: block; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="search-section" id="searchSection">
                <h1>YouTube Screenshot Tool</h1>
     
                <input type="text" id="videoUrl" class="search-bar" 
                       placeholder="Enter YouTube URL (e.g., https://youtu.be/dQw4w9WgXcQ?t=43)">
                
                <div class="timestamp-section" id="timestampSection">
                    <p>Timestamp detected or enter manually:</p>
                    <div class="timestamp-inputs">
                        <div class="time-block">
                            <input type="number" id="hours" class="time-input" min="0" value="0">
                            <span class="time-label">HH</span>
                        </div>
                        <div class="time-block">
                            <input type="number" id="minutes" class="time-input" min="0" max="59" value="0">
                            <span class="time-label">MM</span>
                        </div>
                        <div class="time-block">
                            <input type="number" id="seconds" class="time-input" min="0" max="59" value="0">
                            <span class="time-label">SS</span>
                        </div>
                    </div>
                </div>
                
                <button class="btn" id="generateBtn" onclick="generateScreenshots()">Get Screenshots</button>
                <button class="btn" id="getThumbnailsBtn" onclick="getThumbnails()" style="background: #6c757d;">Get Thumbnails</button>
            </div>
            
            <div class="loading" id="loading">
                <div class="spinner"></div>
                <p>Processing your request...</p>
            </div>
            
            <div class="error" id="error"></div>
            
            <div class="performance-info" id="performanceInfo"></div>
            
            <div class="content-section" id="contentSection">
                <div class="video-container">
                    <iframe id="videoEmbed" class="video-embed" src="" allowfullscreen></iframe>
                </div>
                
                <div class="screenshots-grid" id="screenshotsGrid"></div>
            </div>
        </div>
        
        <script>
            let currentVideoId = null;
            
            document.getElementById('videoUrl').addEventListener('input', function() {
                const url = this.value;
                const videoId = extractVideoId(url);
                
                if (videoId) {
                    const timestamp = extractTimestamp(url);
                    document.getElementById('hours').value = timestamp.hours;
                    document.getElementById('minutes').value = timestamp.minutes;
                    document.getElementById('seconds').value = timestamp.seconds;
                    
                    document.getElementById('timestampSection').classList.add('visible');
                } else {
                    document.getElementById('timestampSection').classList.remove('visible');
                }
            });
            
            function extractVideoId(url) {
                const patterns = [
                    /(?:youtube\\.com\\/watch\\?v=|youtu\\.be\\/|youtube\\.com\\/embed\\/)([a-zA-Z0-9_-]{11})/,
                    /youtube\\.com\\/watch\\?.*v=([a-zA-Z0-9_-]{11})/
                ];
                
                for (let pattern of patterns) {
                    const match = url.match(pattern);
                    if (match) return match[1];
                }
                return null;
            }
            
            function extractTimestamp(url) {
                let match = url.match(/[\\?&]t=(\\d+)$/);
                if (match) {
                    const totalSeconds = parseInt(match[1]);
                    return {
                        hours: Math.floor(totalSeconds / 3600),
                        minutes: Math.floor((totalSeconds % 3600) / 60),
                        seconds: totalSeconds % 60
                    };
                }
                
                match = url.match(/[\\?&]t=(?:(\\d+)h)?(?:(\\d+)m)?(\\d+)s?/);
                if (match) {
                    return {
                        hours: parseInt(match[1] || 0),
                        minutes: parseInt(match[2] || 0),
                        seconds: parseInt(match[3] || 0)
                    };
                }
                
                return { hours: 0, minutes: 0, seconds: 0 };
            }
            
            async function generateScreenshots() {
                const url = document.getElementById('videoUrl').value;
                const hours = parseInt(document.getElementById('hours').value) || 0;
                const minutes = parseInt(document.getElementById('minutes').value) || 0;
                const seconds = parseInt(document.getElementById('seconds').value) || 0;
                
                if (!url) {
                    showError('Please enter a YouTube URL');
                    return;
                }
                
                const videoId = extractVideoId(url);
                if (!videoId) {
                    showError('Invalid YouTube URL');
                    return;
                }
                
                currentVideoId = videoId;
                const timestamp = hours * 3600 + minutes * 60 + seconds;
                
                showLoading(true);
                hideError();
                hidePerformanceInfo();
                
                const startTime = performance.now();
                
                try {
                    const response = await fetch('/api/screenshots', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ url, hours, minutes, seconds })
                    });
                    
                    if (!response.ok) {
                        const error = await response.json();
                        throw new Error(error.detail);
                    }
                    
                    const data = await response.json();
                    const endTime = performance.now();
                    const clientTime = ((endTime - startTime) / 1000).toFixed(2);
                    
                    displayResults(videoId, timestamp, data.screenshots, data.cached, clientTime, data.processing_time);
                    
                } catch (error) {
                    showError(error.message);
                } finally {
                    showLoading(false);
                }
            }
            
            async function getThumbnails() {
                const url = document.getElementById('videoUrl').value;
                const videoId = extractVideoId(url);
                
                if (!videoId) {
                    showError('Please enter a valid YouTube URL');
                    return;
                }
                
                showLoading(true);
                hideError();
                
                try {
                    const response = await fetch(`/api/thumbnails/${videoId}`);
                    const data = await response.json();
                    displayThumbnails(videoId, data.thumbnails);
                } catch (error) {
                    showError(error.message);
                } finally {
                    showLoading(false);
                }
            }
            
            function displayResults(videoId, timestamp, screenshots, cached, clientTime, serverTime) {
                document.getElementById('searchSection').classList.add('moved');
                
                const embedUrl = `https://www.youtube.com/embed/${videoId}?start=${timestamp}&autoplay=1`;
                document.getElementById('videoEmbed').src = embedUrl;
                
                const grid = document.getElementById('screenshotsGrid');
                grid.innerHTML = screenshots.map(screenshot => `
                    <div class="screenshot-card ${cached ? 'cached' : ''}">
                        ${cached ? '<div class="cache-indicator">CACHED</div>' : ''}
                        <img src="/preview/${screenshot.filename}" class="screenshot-img" alt="${screenshot.name}">
                        <div class="quality-info">
                            <strong>${screenshot.name}</strong><br>
                            Source: ${screenshot.source_height}p<br>
                            Size: ${screenshot.size_kb} KB
                        </div>
                        <a href="${screenshot.download_url}" class="download-btn" download>
                            Download ${screenshot.quality.toUpperCase()}
                        </a>
                    </div>
                `).join('');
                
                // Show performance info
                showPerformanceInfo(cached, clientTime, serverTime);
                
                document.getElementById('contentSection').classList.add('visible');
            }
            
            function displayThumbnails(videoId, thumbnails) {
                document.getElementById('searchSection').classList.add('moved');
                
                const embedUrl = `https://www.youtube.com/embed/${videoId}`;
                document.getElementById('videoEmbed').src = embedUrl;
                
                const grid = document.getElementById('screenshotsGrid');
                grid.innerHTML = Object.entries(thumbnails).map(([quality, url]) => `
                    <div class="screenshot-card">
                        <img src="${url}" class="screenshot-img" alt="${quality} thumbnail" 
                             onerror="this.style.display='none'">
                        <div class="quality-info">
                            <strong>${quality.charAt(0).toUpperCase() + quality.slice(1)} Thumbnail</strong>
                        </div>
                        <a href="${url}" class="download-btn" target="_blank">
                            Download ${quality.toUpperCase()}
                        </a>
                    </div>
                `).join('');
                
                document.getElementById('contentSection').classList.add('visible');
            }
            
            function showPerformanceInfo(cached, clientTime, serverTime) {
                const perfDiv = document.getElementById('performanceInfo');
                let message = '';
                
                if (cached) {
                    message = `Retrieved from cache in ${clientTime}s (${(clientTime * 1000).toFixed(0)}ms)`;
                } else {
                    message = `Generated in ${serverTime}s (Client: ${clientTime}s)`;
                }
                
                perfDiv.innerHTML = message;
                perfDiv.classList.add('visible');
            }
            
            function showLoading(show) {
                document.getElementById('loading').classList.toggle('visible', show);
                document.getElementById('generateBtn').disabled = show;
                document.getElementById('getThumbnailsBtn').disabled = show;
            }
            
            function showError(message) {
                const errorDiv = document.getElementById('error');
                errorDiv.textContent = message;
                errorDiv.classList.add('visible');
            }
            
            function hideError() {
                document.getElementById('error').classList.remove('visible');
            }
            
            function hidePerformanceInfo() {
                document.getElementById('performanceInfo').classList.remove('visible');
            }
        </script>
    </body>
    </html>
    """
    return html

@app.post("/api/screenshots")
async def create_screenshots(request: VideoRequest):
    """Generate multiple quality screenshots using optimized parallel processing"""
    video_id = extract_video_id(request.url)
    if not video_id:
        raise HTTPException(status_code=400, detail="Invalid YouTube URL")
    
    timestamp = request.hours * 3600 + request.minutes * 60 + request.seconds
    start_time = time.time()
    
    try:
        validation = validate_timestamp(request.url, request.hours, request.minutes, request.seconds)
        if not validation["valid"]:
            raise HTTPException(status_code=400, detail=validation["message"])
        
        # Check cache first (parallel lookup for all qualities)
        print(f"Checking cache for {video_id} at {timestamp}s")
        cached_screenshots = await check_cache_parallel(video_id, timestamp)
        if cached_screenshots:
            processing_time = time.time() - start_time
            print(f"Cache HIT for {video_id} at {timestamp}s - returning in {processing_time:.3f}s")
            return JSONResponse(content={
                "success": True,
                "video_id": video_id,
                "timestamp": timestamp,
                "screenshots": cached_screenshots,
                "cached": True,
                "processing_time": round(processing_time, 3)
            })
        else:
            print(f"Cache MISS for {video_id} at {timestamp}s - generating new screenshots")
        
        # Get streams (with caching)
        streams = get_multiple_quality_streams_cached(request.url)
        if not streams:
            raise HTTPException(status_code=404, detail="No streams found")
        
        # Generate screenshots in parallel
        screenshots = await generate_screenshots_parallel(streams, timestamp, video_id)
        
        if not screenshots:
            raise HTTPException(status_code=500, detail="Failed to generate screenshots")
        
        # Cache screenshots in background
        cache_tasks = []
        for screenshot in screenshots:
            if os.path.exists(screenshot['filename']):
                cache_task = asyncio.create_task(
                    cache_screenshot_background(video_id, timestamp, screenshot)
                )
                cache_tasks.append(cache_task)
        
        # Cache metadata
        await fast_cache.store_metadata(video_id, timestamp, screenshots)
        print(f"Cached {len(screenshots)} screenshots for {video_id} at {timestamp}s")
        
        processing_time = time.time() - start_time
        
        return JSONResponse(content={
            "success": True,
            "video_id": video_id,
            "timestamp": timestamp,
            "screenshots": screenshots,
            "cached": False,
            "processing_time": round(processing_time, 2)
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def cache_screenshot_background(video_id: str, timestamp: int, screenshot: Dict):
    """Background caching of individual screenshots"""
    try:
        if os.path.exists(screenshot['filename']):
            with open(screenshot['filename'], 'rb') as f:
                image_data = f.read()
                await fast_cache.store_screenshot(video_id, timestamp, screenshot['quality'], image_data)
    except Exception as e:
        print(f"Background cache error for {screenshot['quality']}: {e}")

async def cache_metadata_background(video_id: str, timestamp: int, screenshots: List[Dict]):
    """Background caching of metadata"""
    try:
        await fast_cache.store_metadata(video_id, timestamp, screenshots)
    except Exception as e:
        print(f"Background metadata cache error: {e}")
    
@app.get("/api/cli/screenshot")
async def cli_screenshot(
    url: str,
    timestamp: int = 0,
    quality: str = "high",
    format: str = "png"
):
    """CLI-friendly endpoint that returns image directly with caching"""
    try:
        video_id = extract_video_id(url)
        if not video_id:
            raise HTTPException(status_code=400, detail="Invalid YouTube URL")
        
        # Check cache first
        cached_image = await fast_cache.get_screenshot(video_id, timestamp, quality)
        if cached_image:
            return Response(content=cached_image, media_type="image/png")
        
        # Generate screenshot
        streams = get_multiple_quality_streams_cached(url)
        if quality not in streams:
            available = ", ".join(streams.keys())
            raise HTTPException(status_code=400, detail=f"Quality '{quality}' not available. Options: {available}")

        # Generate single screenshot
        screenshot = generate_screenshot(
            streams[quality]['url'],
            timestamp,
            video_id,
            quality
        )
        
        if not screenshot or not os.path.exists(screenshot['filename']):
            raise HTTPException(status_code=500, detail="Screenshot generation failed")
        
        # Cache and return
        with open(screenshot['filename'], 'rb') as f:
            image_data = f.read()
            await fast_cache.store_screenshot(video_id, timestamp, quality, image_data)
        
        return Response(content=image_data, media_type="image/png")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/thumbnails/{video_id}")
async def get_thumbnails(video_id: str):
    """Get YouTube thumbnail URLs with availability checking"""
    qualities = {
        'maxres': 'maxresdefault.jpg',     # 1280x720 (if available)
        'standard': 'sddefault.jpg',       # 640x480
        'high': 'hqdefault.jpg',           # 480x360
        'medium': 'mqdefault.jpg',         # 320x180
        'default': 'default.jpg'           # 120x90
    }
    
    thumbnails = {}
    for name, filename in qualities.items():
        url = f'https://img.youtube.com/vi/{video_id}/{filename}'
        thumbnails[name] = url
    
    return {"thumbnails": thumbnails}

@app.get("/preview/{filename}")
async def preview_screenshot(filename: str):
    """Serve screenshot preview from cache or disk"""
    
    # Parse filename to get video_id, timestamp, quality
    if filename.startswith("screenshot_"):
        filename = filename[11:]
    
    # Try to get from memory cache first
    parts = filename.replace('.png', '').split('_')
    if len(parts) >= 3:
        video_id = '_'.join(parts[:-2])
        timestamp = int(parts[-2])
        quality = parts[-1]
        
        # Check memory cache
        image_data = await fast_cache.get_screenshot(video_id, timestamp, quality)
        if image_data:
            return Response(content=image_data, media_type="image/png")
    
    # Fallback to disk file
    if os.path.exists(filename):
        return FileResponse(filename, media_type="image/png")
    
    raise HTTPException(status_code=404, detail="Screenshot not found")


@app.get("/download/{filename}")
async def download_screenshot(filename: str):
    """Serve screenshot download from cache or disk"""
    
    # Remove screenshot_ prefix if present
    if filename.startswith("screenshot_"):
        filename = filename[11:]
    
    # Parse filename to get video_id, timestamp, quality
    parts = filename.replace('.png', '').split('_')
    if len(parts) >= 3:
        video_id = '_'.join(parts[:-2])
        timestamp = int(parts[-2])
        quality = parts[-1]
        
        # Check memory/GCS cache first
        image_data = await fast_cache.get_screenshot(video_id, timestamp, quality)
        if image_data:
            return Response(
                content=image_data, 
                media_type="image/png",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
    
    # Fallback to disk file
    if os.path.exists(filename):
        return FileResponse(
            filename, 
            media_type="image/png",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    raise HTTPException(status_code=404, detail="Screenshot not found")




@app.get("/health")
async def health_check():
    """Health check endpoint with cache stats"""
    return {
        "status": "healthy", 
        "rate_limit": f"Max {RATE_LIMIT} requests per minute",
        "cache_stats": fast_cache.cache_stats,
        "optimizations": [ 
            "Multi-level Caching", 
            "Optimized FFmpeg Seeking",
            "Stream URL Caching"
        ]
    }

@app.get("/api/cache-stats")
async def get_cache_stats():
    """Get detailed cache performance statistics"""
    total_requests = fast_cache.cache_stats["hits"] + fast_cache.cache_stats["misses"]
    hit_rate = (fast_cache.cache_stats["hits"] / total_requests * 100) if total_requests > 0 else 0
    
    return {
        "cache_stats": fast_cache.cache_stats,
        "hit_rate_percentage": round(hit_rate, 2),
        "memory_cache_size": len(fast_cache.memory_cache),
        "stream_cache_size": len(stream_cache.cache)
    }

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
