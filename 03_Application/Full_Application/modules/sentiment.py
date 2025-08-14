# modules/sentiment.py
import boto3, json
import pandas as pd
from functools import lru_cache
from transformers import pipeline
from modules.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SENT_BUCKET, SENT_PREFIX, SENT_KEYWORDS

@lru_cache(maxsize=None)
def load_sentiment_posts(bucket: str, prefix: str) -> pd.DataFrame:
    client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    posts = []
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith(".json"):
            raw = client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            p = json.loads(raw)
            text = p.get("text","")
            posts.append({
                "account": p.get("author", {}).get("handle"),
                "descripcion": text,
                "menciona": any(k in text.lower() for k in SENT_KEYWORDS)
            })
    return pd.DataFrame(posts)

@lru_cache(maxsize=1)
def get_sentiment_model():
    return pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

