import time
import requests
import os
from datetime import datetime, timedelta, timezone

def call_run_jobs():
    """Call the run-jobs endpoint"""
    url = f"{os.getenv('RENDER_SERVICE_URL')}/run-jobs"
    headers = {'X-API-Key': os.getenv('BACKEND_API_KEY')}
    
    try:
        print(f"🚀 Calling {url} at {datetime.now(timezone.utc)}")
        response = requests.get(url, headers=headers, timeout=300)
        if response.status_code == 200:
            print(f"✅ Success: {response.json()}")
        else:
            print(f"❌ Failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    print("🔄 Starting lightweight scheduler...")
    
    while True:
        now = datetime.now(timezone.utc)
        next_run = now.replace(hour=11, minute=0, second=0, microsecond=0)
        if now.hour >= 11:
            next_run += timedelta(days=1)
        
        sleep_seconds = (next_run - now).total_seconds()
        print(f"💤 Sleeping {sleep_seconds/3600:.1f}h until {next_run} UTC")
        
        time.sleep(sleep_seconds)
        call_run_jobs()

if __name__ == "__main__":
    main()