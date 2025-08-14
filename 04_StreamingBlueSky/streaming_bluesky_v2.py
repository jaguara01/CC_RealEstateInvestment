import asyncio
import websockets
import json
import requests
import os
from pathlib import Path
import uuid
from io import BytesIO
from PIL import Image
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from websockets.exceptions import ConnectionClosedError
import boto3
from botocore.exceptions import ClientError



# Load credentials from .env
load_dotenv()
BSKY_USERNAME = os.getenv("ATP_EMAIL")
BSKY_PASSWORD = os.getenv("ATP_PASSWORD")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET", "bluesky-streamer")



POSTS_DIR = Path("posts_bluesky")
IMAGES_DIR = Path("images_bluesky")
IMAGES_DIR.mkdir(parents=True, exist_ok=True)



# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)



# Target media DIDs
TARGET_DIDS = {
    "did:plc:llz4zgd7k25wwgx4qqtct6nn": "idealista.bsky.social",
    "did:plc:7nzikvincp6itirgingentvj": "postnormcore.bsky.social",
    "did:plc:qgifegezoygvscwc3wwwsbhb": "javigil.bsky.social",
    "did:plc:7kfe3wukoqvq7ucgtulaxvzn": "carolinaom89.bsky.social",
    "did:plc:u6mkbcgviwlbhuwqirmhcgu3": "elpais.com",
    "did:plc:humoyyfleayy76szpd5nqmw5": "catalannews.com",
    "did:plc:ylspytpr7posthjximkqea4n": "acn.cat",
    "did:plc:reloczc52dp4zqwymggpepoz": "vilaweb.cat",
    "did:plc:pfyxqe5aivngt44dqojq6pyq": "igualadanews.bsky.social",
    "did:plc:fejefvoxntmzw7htgoxivx26": "institutmetropoli.cat",
    "did:plc:7y5fmnz4ftxjobz6domosi56": "negg47.bsky.social",
    "did:plc:7zlnvtvm2fqq4wj5m43im34y": "elpaismexico.bsky.social",
    "did:plc:nue3a4qhd2wheedhsniw66vu": "elpaisamerica.bsky.social"
}



# Get JWT token
def obtener_token():
    try:
        res = requests.post(
            "https://bsky.social/xrpc/com.atproto.server.createSession",
            json={"identifier": BSKY_USERNAME, "password": BSKY_PASSWORD}
        )
        res.raise_for_status()
        return res.json()["accessJwt"]
    except Exception as e:
        print(f"Login error: {e}")
        return None



# Get extra post details
def obtener_detalles_extra(uri, token):
    if not token:
        print("Token unavailable, cannot fetch post details.")
        return {}

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://bsky.social/xrpc/app.bsky.feed.getPostThread?uri={uri}"

    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        post = data.get("thread", {}).get("post", {})
        author = post.get("author", {})
        return {
            "likes": post.get("likeCount", 0),
            "reposts": post.get("repostCount", 0),
            "replies": post.get("replyCount", 0),
            "author_handle": author.get("handle"),
            "author_displayName": author.get("displayName"),
            "author_avatar": author.get("avatar"),
        }
    except requests.exceptions.HTTPError as e:
        if r.status_code == 401:
            print("Token expired or invalid. Retrying...")
            new_token = obtener_token()
            return obtener_detalles_extra(uri, new_token)
        print(f"Error fetching post details: {e}")
        return {}
    except Exception as e:
        print(f"Unexpected error fetching details: {e}")
        return {}



# WebSocket connection to Jetstream stream
async def listen_bluesky_jetstream():
    while True:
        token = obtener_token()
        if not token:
            print("Could not obtain token. Retrying in 15 seconds...")
            await asyncio.sleep(15)
            continue

        wanted_dids_query = "&".join([f"wantedDids={did}" for did in TARGET_DIDS])
        url = f"wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&{wanted_dids_query}"

        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=60) as websocket:
                print(f"Connected to Jetstream. Listening for posts from:\n{TARGET_DIDS}\n")

                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)
                        post_did = data.get("did")

                        if post_did in TARGET_DIDS and "commit" in data:
                            commit_data = data["commit"]
                            if commit_data.get("operation") != "create":
                                continue
                            if commit_data.get("collection") != "app.bsky.feed.post":
                                continue

                            post_record = commit_data.get("record", {})
                            post_uri = f"at://{post_did}/app.bsky.feed.post/{commit_data.get('rkey')}"
                            timestamp = post_record.get("createdAt", datetime.utcnow().isoformat())
                            text = post_record.get("text", "[No Content]")
                            source = TARGET_DIDS[post_did]

                            # Get extended details (author, metrics)
                            extra = obtener_detalles_extra(post_uri, token)

                            # Get images
                            media_urls = []
                            embed = post_record.get("embed")
                            if embed:
                                embed_type = embed.get("$type")
                                if embed_type == "app.bsky.embed.images":
                                    media_urls = [img.get("fullsize") for img in embed.get("images", [])]
                                elif embed_type == "app.bsky.embed.recordWithMedia":
                                    media = embed.get("media", {})
                                    if media.get("$type") == "app.bsky.embed.images":
                                        media_urls = [img.get("fullsize") for img in media.get("images", [])]
                                elif embed_type == "app.bsky.embed.external":
                                    media_urls.append(embed.get("external", {}).get("thumb"))

                            # Create JSON for Kafka
                            post_json = {
                                "timestamp": timestamp,
                                "text": text,
                                "uri": post_uri,
                                "source": source,
                                "media_urls": media_urls,
                                "author": {
                                    "handle": extra.get("author_handle"),
                                    "displayName": extra.get("author_displayName"),
                                    "avatar": extra.get("author_avatar")
                                },
                                "metrics": {
                                    "likes": extra.get("likes"),
                                    "reposts": extra.get("reposts"),
                                    "replies": extra.get("replies")
                                }
                            }

                            base_name = guardar_post_en_json(post_json)
                            procesar_post_y_descargar_imagen(post_json, base_name)

                    except json.JSONDecodeError:
                        print("JSON decoding error")
                    except Exception as e:
                        print(f"Unexpected error processing message: {e}")

        except ConnectionClosedError as e:
            print(f"WebSocket closed (1011): {e}")
        except Exception as e:
            print(f"Error connecting WebSocket: {e}")

        print("Retrying connection in 10 seconds...\n")
        await asyncio.sleep(10)



def upload_to_s3(data, key, content_type='application/json'):
    """Upload data to S3 bucket"""
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=data,
            ContentType=content_type
        )
        print(f"✅ Uploaded to S3: {key}")
        return True
    except ClientError as e:
        print(f"❌ Error uploading to S3: {e}")
        return False



def guardar_post_en_json(post_json, carpeta="posts_bluesky"):
    Path(carpeta).mkdir(parents=True, exist_ok=True)
    timestamp = post_json.get("timestamp", datetime.utcnow().isoformat())
    safe_timestamp = timestamp.replace(":", "-")
    base_name = f"{safe_timestamp}_{uuid.uuid4().hex[:8]}"
    ruta_archivo = Path(carpeta) / f"{base_name}.json"

    # Guardar localmente
    with open(ruta_archivo, "w", encoding="utf-8") as f:
        json.dump(post_json, f, ensure_ascii=False, indent=2)
    print(f"✅ Json guardado localmente.")

    # Guardar en S3
    json_data = json.dumps(post_json, ensure_ascii=False, indent=2)
    s3_key = f"posts_bluesky/{base_name}.json"
    upload_to_s3(json_data, s3_key)

    return base_name



def download_image(url, local_path):
    try:
        response = requests.get(url)
        response.raise_for_status()
        # Guardar localmente
        with open(local_path, "wb") as f:
            f.write(response.content)
        return response.content
    except Exception as e:
        print(f"❌ Error downloading {url}: {e}")
        return None



def extract_image_metadata(img_bytes):
    """Extract metadata from image bytes"""
    try:
        img = Image.open(BytesIO(img_bytes))
        width, height = img.size
        size_kb = len(img_bytes) / 1024
        return width, height, size_kb
    except Exception as e:
        print(f"❌ Error extracting metadata: {e}")
        return None, None, None



def procesar_post_y_descargar_imagen(post, base_name):
    """Process post and download images to S3"""
    media_items = post.get("media_urls", [])

    for idx, media in enumerate(media_items):
        if isinstance(media, dict) and media.get("$type") == "blob":
            cid = media.get("ref", {}).get("$link")
            if not cid:
                print(f"⚠️ CID not found in blob")
                continue
            post_did = post.get("uri", "").split("/")[2]
            image_url = f"https://bsky.social/xrpc/com.atproto.sync.getBlob?did={post_did}&cid={cid}"
        elif isinstance(media, str):
            image_url = media
        else:
            print(f"⚠️ Invalid media format: {media}")
            continue

        filename = f"{base_name}_{idx}.jpg"
        image_path = IMAGES_DIR / filename
        image_bytes = download_image(image_url, image_path)
        if not image_bytes:
            continue

        # Subir a S3
        s3_key = f"images_bluesky/{filename}"
        upload_to_s3(image_bytes, s3_key, 'image/jpeg')

        width, height, size_kb = extract_image_metadata(image_bytes)
        print(f"✅ Imagen guardada localmente y en S3: {filename} | {width}x{height}px, {size_kb:.1f} KB")



async def main():
    while True:
        try:
            await listen_bluesky_jetstream()
        except ConnectionClosedError as e:
            print(f"WebSocket unexpectedly closed: {e}")
            print("Reconnecting in 10 seconds...\n")
            await asyncio.sleep(10)
        except Exception as e:
            print(f"Unexpected error: {e}")
            print("Retrying in 10 seconds...\n")
            await asyncio.sleep(10)



# Run
asyncio.run(main())
