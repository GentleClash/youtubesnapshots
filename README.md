# YouTube Snapshots

A powerful tool to extract high-quality screenshots from YouTube videos at specific timestamps. This service allows you to grab screenshots in multiple quality levels (Ultra/1080p, High/720p, Medium/480p, Low/360p) from any YouTube video.

## Features

- 🖼️ Extract screenshots in multiple quality levels (1080p, 720p, 480p, 360p)
- ⏱️ Specify exact timestamps using hours, minutes, and seconds
- 🚀 Fast multi-level caching system (memory + file-based)
- 🔄 Support for both local file cache and Google Cloud Storage (GCS) cache
- 🌐 Simple web interface and API endpoints
- 📦 Easy deployment with Docker

## Prerequisites

- Python 3.13+ (for direct installation)
- FFmpeg
- Docker (for containerized deployment)
- yt-dlp

## Self-Hosting Options

### Option 1: Using Docker (Recommended)

The easiest way to run YouTube Snapshots is using Docker:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/GentleClash/youtubesnapshots.git
   cd youtubesnapshots
   ```

2. **Build the Docker image**:
   ```bash
   docker build -t youtubesnapshots .
   ```

3. **Run the container**:
   ```bash
   docker run -d --name youtube-snapshots -p 8000:8000 -v $(pwd)/cache:/app/cache youtubesnapshots
   ```

4. **Access the application**:
   Open your browser and navigate to `http://localhost:8000`

### Option 2: Direct Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/GentleClash/youtubesnapshots.git
   cd youtubesnapshots
   ```

2. **Install FFmpeg** (if not already installed):
   ```bash
   # For Ubuntu/Debian
   sudo apt-get update && sudo apt-get install -y ffmpeg
   
   # For Fedora
   sudo dnf install ffmpeg
   
   # For macOS with Homebrew
   brew install ffmpeg
   ```

3. **Create and activate a virtual environment** (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

4. **Install the dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

5. **Run the application**:
   ```bash
   uvicorn app:app --host 0.0.0.0 --port 8000
   ```

6. **Access the application**:
   Open your browser and navigate to `http://localhost:8000`

## Environment Variables

You can configure the application using these environment variables:

| Variable | Description | Default Value |
|----------|-------------|---------------|
| `LOCAL_CACHE_DIR` | Directory for local cache storage | `cache` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to Google Cloud service account key file (for GCS cache) | None |

### Setting up Google Cloud Storage (Optional)

1. Create a service account key in Google Cloud Console with Storage Admin permissions
2. Download the JSON key file
3. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of this file:

   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service.json"
   ```

## API Usage

### Get Screenshots

```
POST /api/screenshots

Body:
{
  "url": "https://www.youtube.com/watch?v=VIDEO_ID",
  "timestamp": 120,  // or use hours, minutes, seconds fields
  "hours": 0,
  "minutes": 2,
  "seconds": 0
}
```

### Get CLI Screenshot

```
GET /api/cli/screenshot?url=https://www.youtube.com/watch?v=VIDEO_ID&timestamp=120&quality=high
```

Available qualities: `ultra`, `high`, `medium`, `low`

## Cache Structure

The application uses a multi-level caching system:

1. **In-memory cache**: Fastest, but cleared when the application restarts
2. **Local file cache**: Persistent storage in the `cache` directory:
   - `cache/screenshots/`: Screenshot images
   - `cache/metadata/`: JSON metadata for screenshots

## Troubleshooting

- **ffmpeg not found**: Ensure FFmpeg is installed and accessible in your system PATH
- **yt-dlp errors**: YouTube may occasionally block requests; try again later or use a different IP address
- **Permission errors**: Check file/directory permissions, especially when using Docker volumes

## Credits

- [yt-dlp](https://github.com/yt-dlp/yt-dlp): For video information extraction
- [FFmpeg](https://ffmpeg.org/): For screenshot generation

## License

See the [LICENSE](LICENSE) file for details.
