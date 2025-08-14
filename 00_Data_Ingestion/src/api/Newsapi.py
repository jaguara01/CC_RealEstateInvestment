import requests
import csv
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import time
import utils

# Calculate the date one year ago from today
one_month_ago = (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d")

# Load environment variables
# load_dotenv(os.path.join(os.getcwd(), 'newsapi.env'))  # Adjust the path if necessary
NEWSAPI_API_KEY = os.getenv("NEWSAPI_API_KEY")

# NewsAPI endpoint and parameters
url = (
    "https://newsapi.org/v2/everything?"
    "q=Barcelona&"
    f"from={one_month_ago}&"
    "sortBy=popularity&"
    f"apiKey={NEWSAPI_API_KEY}"
)


def export_to_csv(local_filename):
    try:
        # Make the API request
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            exit()

        data = response.json()

        # Check if articles were found
        if data["totalResults"] == 0:
            print("No articles found.")
            exit()

        # Prepare CSV file
        csv.field_size_limit(2**22)

        with open(local_filename, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "Title",
                "Description",
                "URL",
                "Published_At",
                "Source",
                "Content",
                "Language",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()

            # Process articles
            for article in data["articles"]:
                writer.writerow(
                    {
                        "Title": article.get("title", ""),
                        "Description": article.get("description", ""),
                        "URL": article.get("url", ""),
                        "Published_At": article.get("publishedAt", ""),
                        "Source": article.get("source", {}).get("name", ""),
                        "Content": article.get("content", ""),
                        "Language": article.get("language", ""),  # Add language column
                    }
                )

            print(
                f"Successfully saved {len(data['articles'])} articles to {local_filename}"
            )

    except Exception as e:
        print(f"An error occurred: {str(e)}")


# Main execution
if __name__ == "__main__":
    try:
        local_filename = "barcelona_real_estate_news.csv"
        export_to_csv(local_filename)
        s3 = utils.initialize_s3()
        utils.upload_to_s3(
            s3,
            local_filename,
            f"news/newsapi/{local_filename}",
            "02-semistructured-data",
        )
    except Exception as e:
        print(f"An error occurred: {e}")
else:
    print("No posts found to export.")
