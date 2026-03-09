import functions_framework
import os
import datetime
import requests
import json
from google.cloud import storage
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

@functions_framework.http
def ingest_trip_data(request):
    try:
        # 1. Configuration
        bucket_name = os.environ.get('BUCKET_NAME', 'nsw-trains-analytics-bronze')
        api_key = os.environ.get('TRANSPORT_NSW_API_KEY')
        
        if not api_key:
            return "Error: TRANSPORT_NSW_API_KEY missing.", 500

        # 2. Time Partitioning
        now = datetime.datetime.now()
        partition_date = now.strftime("%Y-%m-%d")
        timestamp = now.strftime("%H%M%S")
        
        # 3. Fetch from API (Protobuf)
        url = "https://api.transport.nsw.gov.au/v2/gtfs/vehiclepos/sydneytrains"
        headers = {
            "Authorization": f"apikey {api_key}",
            "Accept": "application/x-google-protobuf" # Request Binary
        }
        
        print(f"Fetching Protobuf from {url}...")
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            # 4. Parse Protobuf (The "Senior" Step)
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            
            # Convert to Python Dict -> JSON
            # We use MessageToDict to safely convert the complex Protobuf object
            feed_dict = MessageToDict(feed)
            
            # 5. Save as JSON (Ready for BigQuery)
            # Filename: trips_HHMMSS.json
            filename = f"realtime_trip_updates/nsw_trains/dt={partition_date}/trips_{timestamp}.json"
            
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(filename)
            
            blob.upload_from_string(
                json.dumps(feed_dict), 
                content_type='application/json'
            )
            
            return f"Success: Converted PB to JSON and saved to gs://{bucket_name}/{filename}", 200
            
        else:
            return f"API Error {response.status_code}: {response.text}", 502

    except Exception as e:
        return f"Internal Error: {e}", 500