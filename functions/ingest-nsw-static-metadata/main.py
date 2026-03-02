import functions_framework
import requests
import zipfile
import io
import os
from google.cloud import storage

@functions_framework.http
def sync_static_gtfs(request):
    """
    Moving initialization INSIDE the function allows the container 
    to 'Start' and 'Listen' successfully before attempting heavy logic.
    """
    try:
        # 1. Initialize inside the request scope
        storage_client = storage.Client()
        bucket_name = os.environ.get('BUCKET_NAME', 'nsw-trains-analytics-bronze')
        api_key = os.environ.get('TRANSPORT_NSW_API_KEY')
        
        if not api_key:
            return "Missing NSW_API_KEY environment variable", 401

        url = "https://api.transport.nsw.gov.au/v1/gtfs/schedule/sydneytrains"
        headers = {"Authorization": f"apikey {api_key}"}

        # 2. Performance: Use stream=True to keep memory footprint low
        print("Downloading static GTFS bundle...")
        with requests.get(url, headers=headers, stream=True) as r:
            r.raise_for_status()
            zip_data = io.BytesIO(r.content)
            
        with zipfile.ZipFile(zip_data) as z:
            target_files = ['stops.txt', 'routes.txt', 'trips.txt', 'stop_times.txt']
            bucket = storage_client.bucket(bucket_name)
            
            for file_name in target_files:
                if file_name in z.namelist():
                    content = z.read(file_name)
                    blob = bucket.blob(f"static_files/static_gtfs/{file_name}")
                    blob.upload_from_string(content, content_type='text/plain')
                    print(f"Overwritten: {file_name}")

        return "Static Sync Complete", 200

    except Exception as e:
        print(f"Function Error: {str(e)}")
        return f"Error: {str(e)}", 500