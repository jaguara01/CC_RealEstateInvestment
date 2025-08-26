import utils

if __name__ == "__main__":
    local_filename = "search_name.json"
    s3_key = f"google-maps/{local_filename}"
    bucket_name = "02-semistructured-data"

    try:
        s3 = utils.initialize_s3()
        utils.upload_to_s3(s3, local_filename, s3_key, bucket_name)
    except Exception as e:
        print(f"❌ Upload failed: {e}")
