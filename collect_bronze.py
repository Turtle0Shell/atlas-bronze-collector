import json
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# =====================================================================
# 1. LOAD VERIFIED ACTIVE TILES & ORBIS CONFIG FROM active_tiles_z13.json
# =====================================================================
CONFIG_FILE = "active_tiles_z13.json"

with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    TILE_CONFIG = json.load(f)

API_MODE = TILE_CONFIG.get("api_mode", "orbis_v2")
ZOOM = TILE_CONFIG["zoom"]
BBOX = TILE_CONFIG["bbox"]
ORBIS_V2_ATTRIBUTES = TILE_CONFIG["orbis_v2_attributes"]
ORBIS_V1_TAGS = TILE_CONFIG.get("orbis_v1_tags", ",".join(TILE_CONFIG["all_tags"]))
ACTIVE_TILES_Z13 = [tuple(pair) for pair in TILE_CONFIG["active_tiles"]]


# =====================================================================
# 2. HELPER FUNCTIONS: HTTP SESSION & API KEY SELECTION
# =====================================================================
def get_tomtom_api_key() -> str:
    key1 = os.environ.get("TOMTOM_API_KEY", "").strip()
    key2 = os.environ.get("TOMTOM_API_KEY_2", "").strip()
    if not key1:
        raise ValueError("Missing TOMTOM_API_KEY environment variable.")
    if key2:
        slot = int(datetime.now(timezone.utc).timestamp() // 600)
        return key1 if (slot % 2 == 0) else key2
    return key1


def create_http_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=16, pool_maxsize=16)
    session.mount("https://", adapter)
    return session


# =====================================================================
# 3. TOMTOM ORBIS COLLECTION (VECTOR FLOW TILES + INCIDENT DETAILS)
# =====================================================================
def fetch_single_tile(session: requests.Session, api_key: str, x: int, y: int):
    if API_MODE == "orbis_v2":
        url = f"https://api.tomtom.com/maps/orbis/traffic/flow/vector/tile/{ZOOM}/{x}/{y}"
        params = {
            "apiVersion": "2",
            "key": api_key,
            "attributes": ORBIS_V2_ATTRIBUTES
        }
    else:
        url = f"https://api.tomtom.com/maps/orbis/traffic/tile/flow/{ZOOM}/{x}/{y}.pbf"
        params = {
            "apiVersion": "1",
            "key": api_key,
            "tags": ORBIS_V1_TAGS
        }

    resp = session.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return (x, y, resp.content)


def collect_flow_tiles_zip(session: requests.Session, api_key: str, timestamp_str: str, iso_now: str) -> str:
    zip_filename = f"flow_tiles_{timestamp_str}.zip"
    manifest = {
        "captured_at_utc": iso_now,
        "api_mode": API_MODE,
        "zoom": ZOOM,
        "tags": TILE_CONFIG["all_tags"],
        "total_expected_tiles": len(ACTIVE_TILES_Z13),
        "downloaded_tiles": 0,
        "failed_tiles": []
    }

    with zipfile.ZipFile(zip_filename, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(fetch_single_tile, session, api_key, x, y): (x, y)
                for (x, y) in ACTIVE_TILES_Z13
            }
            for future in as_completed(futures):
                x, y = futures[future]
                try:
                    tx, ty, pbf_bytes = future.result()
                    zf.writestr(f"z{ZOOM}_x{tx}_y{ty}.pbf", pbf_bytes)
                    manifest["downloaded_tiles"] += 1
                except Exception as exc:
                    print(f"[WARN] Failed tile ({x}, {y}): {exc}")
                    manifest["failed_tiles"].append({"x": x, "y": y, "error": str(exc)})

        zf.writestr("manifest.json", json.dumps(manifest, indent=2))

    print(f"[INFO] Packaged {manifest['downloaded_tiles']}/{len(ACTIVE_TILES_Z13)} tiles into {zip_filename}")
    return zip_filename


def collect_incidents_json(session: requests.Session, api_key: str, timestamp_str: str, iso_now: str) -> str:
    json_filename = f"incidents_{timestamp_str}.json"

    # 1. Try Orbis v2 Incident Details (uses timeValidity query param + Accept-Language header)
    url_v2 = "https://api.tomtom.com/maps/orbis/traffic/incidents/details"
    params_v2 = {
        "apiVersion": "2",
        "key": api_key,
        "bbox": BBOX,
        "timeValidity": "present"
    }
    headers_v2 = {
        "Accept": "application/json",
        "Accept-Language": "en-GB"
    }
    resp = session.get(url_v2, params=params_v2, headers=headers_v2, timeout=25)
    used_endpoint = "orbis_v2"

    # 2. Fallback to Orbis v1 Incident Details if v2 returns a 4xx error
    if resp.status_code != 200:
        url_v1 = "https://api.tomtom.com/maps/orbis/traffic/incidentDetails"
        fields_query = (
            "{incidents{type,geometry{type,coordinates},"
            "properties{id,iconCategory,magnitudeOfDelay,"
            "events{description,code,iconCategory},"
            "startTime,endTime,from,to,length,delay,"
            "roadNumbers,timeValidity,probabilityOfOccurrence,"
            "numberOfReports,lastReportTime,"
            "tmc{countryCode,tableNumber,tableVersion,direction,points{location,offset}}}}}"
        )
        params_v1 = {
            "apiVersion": "1",
            "key": api_key,
            "bbox": BBOX,
            "fields": fields_query,
            "language": "en-GB",
            "timeValidityFilter": "present"
        }
        resp = session.get(url_v1, params=params_v1, timeout=25)
        used_endpoint = "orbis_v1"

    resp.raise_for_status()
    raw_payload = resp.json()

    enriched_payload = {
        "captured_at_utc": iso_now,
        "api_mode": used_endpoint,
        "bbox": BBOX,
        "incident_count": len(raw_payload.get("incidents", [])),
        "data": raw_payload
    }

    with open(json_filename, "w", encoding="utf-8") as f:
        json.dump(enriched_payload, f, ensure_ascii=False, indent=2)

    print(f"[INFO] Saved {enriched_payload['incident_count']} incidents into {json_filename} (via {used_endpoint})")
    return json_filename


# =====================================================================
# 4. GOOGLE DRIVE UPLOAD (WITH DAILY YYYY-MM-DD PARTITIONING)
# =====================================================================
def get_drive_service():
    creds = Credentials(
        token=None,
        refresh_token=os.environ["GDRIVE_REFRESH_TOKEN"].strip(),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GDRIVE_CLIENT_ID"].strip(),
        client_secret=os.environ["GDRIVE_CLIENT_SECRET"].strip(),
        scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds)


def get_or_create_daily_folder(service, parent_folder_id: str, date_str: str) -> str:
    folder_name = f"date={date_str}"
    query = (
        f"name = '{folder_name}' and "
        f"'{parent_folder_id}' in parents and "
        f"mimeType = 'application/vnd.google-apps.folder' and "
        f"trashed = false"
    )
    results = service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]

    folder_metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id]
    }
    created = service.files().create(body=folder_metadata, fields="id").execute()
    return created["id"]


def upload_file_to_drive(service, local_path: str, parent_folder_id: str, mime_type: str):
    file_metadata = {
        "name": os.path.basename(local_path),
        "parents": [parent_folder_id]
    }
    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=False)
    uploaded = service.files().create(body=file_metadata, media_body=media, fields="id, name").execute()
    print(f"[INFO] Uploaded {uploaded['name']} to Google Drive (File ID: {uploaded['id']})")


# =====================================================================
# 5. MAIN ENTRYPOINT
# =====================================================================
def main():
    now_utc = datetime.now(timezone.utc)
    timestamp_str = now_utc.strftime("%Y%m%d_%H%M")
    date_str = now_utc.strftime("%Y-%m-%d")
    iso_now = now_utc.isoformat()

    api_key = get_tomtom_api_key()
    session = create_http_session()

    print(f"=== Starting ATLAS Bronze Collection ({iso_now}) ===")
    zip_path = collect_flow_tiles_zip(session, api_key, timestamp_str, iso_now)
    json_path = collect_incidents_json(session, api_key, timestamp_str, iso_now)

    drive_service = get_drive_service()
    flow_root_id = os.environ["GDRIVE_FLOW_FOLDER_ID"].strip()
    incidents_root_id = os.environ["GDRIVE_INCIDENTS_FOLDER_ID"].strip()

    flow_daily_id = get_or_create_daily_folder(drive_service, flow_root_id, date_str)
    incidents_daily_id = get_or_create_daily_folder(drive_service, incidents_root_id, date_str)

    upload_file_to_drive(drive_service, zip_path, flow_daily_id, "application/zip")
    upload_file_to_drive(drive_service, json_path, incidents_daily_id, "application/json")
    print("=== ATLAS Bronze Collection Completed Successfully ===")


if __name__ == "__main__":
    main()
