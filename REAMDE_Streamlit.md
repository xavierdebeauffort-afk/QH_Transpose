# QH Transpose Streamlit App

User-friendly web interface for processing quarter-hourly energy meter data.

## Features

✅ **Drag & Drop File Upload** - Upload multiple CSV files at once
✅ **Auto-Detection** - Automatically detects encoding and file structure
✅ **Date Range Filtering** - Quick year selection or custom date ranges
✅ **Batch Processing** - Process multiple files simultaneously
✅ **Real-time Progress** - Live progress indicators for each file
✅ **Data Quality Checks** - Automatic validation and cleaning
✅ **Instant Download** - Download processed Excel files immediately
✅ **Beautiful UI** - Clean, intuitive interface with color-coded status

## Quick Start

### Option 1: Run Locally

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the app:**
   ```bash
   streamlit run app.py
   ```

3. **Open browser:**
   The app will automatically open at `http://localhost:8501`

### Option 2: Deploy to Streamlit Cloud (Recommended for Sharing)

1. **Push to GitHub:**
   - Create a new GitHub repository
   - Push these files: `app.py`, `requirements.txt`

2. **Deploy on Streamlit Cloud:**
   - Go to [share.streamlit.io](https://share.streamlit.io)
   - Click "New app"
   - Connect your GitHub repository
   - Select the `app.py` file
   - Click "Deploy"

3. **Share the URL:**
   Your colleagues can access the app via the Streamlit Cloud URL without any installation!

## Usage

1. **Upload Files:** Drag and drop CSV files or click to browse
2. **Select Date Range:** Choose a year or set custom dates in the sidebar
3. **Process:** Click "Process Files" button
4. **Download:** Download cleaned Excel files for each processed CSV

## What It Does

The app processes German smart meter CSV files by:
- Detecting file encoding (UTF-8, Latin-1, CP1252)
- Removing mid-file headers automatically
- Filtering for A- and A+ energy direction labels
- Extracting 96 quarter-hourly values per day
- Converting to clean Excel format with timestamps

## File Structure

```
QH_Transpose/
├── app.py                      # Streamlit web app
├── requirements.txt            # Python dependencies
├── QH_Transpose_1.0.py        # Original CLI script (for reference)
└── README_STREAMLIT.md        # This file
```

## Deployment Options

### Streamlit Cloud (Easiest)
- **Cost:** Free
- **Setup:** ~5 minutes
- **Best for:** Sharing with colleagues
- **URL:** Custom subdomain (e.g., `yourapp.streamlit.app`)

### Heroku
```bash
# Install Heroku CLI, then:
heroku create your-app-name
git push heroku main
```

### Docker
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY app.py .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501"]
```

