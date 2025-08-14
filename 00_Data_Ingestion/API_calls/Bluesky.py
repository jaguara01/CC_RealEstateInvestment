from atproto import Client, models
import csv
from datetime import datetime
from dateutil import parser
import sys
import os
from dotenv import load_dotenv
import time
import utils

# Load environment variables from .aws/credentials.env
load_dotenv(os.path.join(os.getcwd(), 'bluesky.env'))  # Adjust the path if necessary

# Initialize the Bluesky client
client = Client()

# Login to Bluesky using your credentials
username = "danirc2.bsky.social"
password = os.getenv("password")
client.login(username, password)

def resolve_handle(handle):
    try:
        response = client.com.atproto.identity.resolve_handle({"handle": handle})
        return response.did
    except Exception as e:
        print(f"Error resolving handle: {e}")
        return None


# Define the feed URLs

feed_mapping = {
    f"at://{resolve_handle('bskyfeeds-by-g.bsky.social')}/app.bsky.feed.generator/aaabstwzakhbc": "Barcelona", # Barcelona feed
    f"at://{resolve_handle('blueskyfeeds.com')}/app.bsky.feed.generator/housing": "Housing and Real Estate" # Housing and Real Estate feed
}

# Function to retrieve posts from a feed
def get_feed_posts(feed_url):
    data = client.app.bsky.feed.get_feed({
        'feed': feed_url,
        'limit': 100,
    }, headers={'Accept-Language': 'en,es,ca'})
    feed = data.feed
    return data.feed, feed_url

def export_to_csv(feed_data, filename):

    csv.field_size_limit(2**22)  
    
    rows = []
    seen_uris = set()
    
    for feed_items, feed_url in feed_data:
        feed_name = feed_mapping.get(feed_url, 'Unknown Feed')
        
        for feed_item in feed_items:
            post = feed_item.post
            author = post.author
            record = post.record
            
            uri = getattr(post, 'uri', '')
            if not uri or uri in seen_uris:
                continue
            seen_uris.add(uri)

            try:
                created_at = parser.parse(getattr(record, 'created_at', '')).isoformat() + 'Z'
            except:
                created_at = ''

            row = {
                'Feed Name': feed_name,
                'Author Handle': getattr(author, 'handle', ''),
                'Display Name': getattr(author, 'display_name', '') or '[No Display Name]',
                'Post Text': getattr(record, 'text', '').replace('\n', '\\n'),
                'Created At': created_at,
                'Likes': getattr(post, 'like_count', 0),
                'Replies': getattr(post, 'reply_count', 0),
                'Reposts': getattr(post, 'repost_count', 0),
                'URI': uri
            }
            rows.append(row)

    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        fieldnames = ['Feed Name', 'Author Handle', 'Display Name', 'Post Text', 
                     'Created At', 'Likes', 'Replies', 'Reposts', 'URI']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, 
                              quoting=csv.QUOTE_ALL,
                              escapechar='\\')
        writer.writeheader()
        writer.writerows(sorted(rows, 
                              key=lambda x: x['Created At'], 
                              reverse=True))

# Main execution
if __name__ == "__main__":
    all_posts = []
    for feed_url, feed_name in feed_mapping.items():
        try:
            feed, feed_url = get_feed_posts(feed_url)
            all_posts.append((feed, feed_url))
        except Exception as e:
            print(f"Error fetching feed {feed_url}: {e}")

    if all_posts:
        try:
            local_filename = "bluesky_posts.csv"
            export_to_csv(all_posts, local_filename)
            print("CSV exported successfully!")
            s3 = utils.initialize_s3()
            utils.upload_to_s3(s3, local_filename, f"social-media/bluesky/{local_filename}", "02-semistructured-data")
        except Exception as e:
            print(f"An error occurred: {e}")
    else:
        print("No posts found to export.")

