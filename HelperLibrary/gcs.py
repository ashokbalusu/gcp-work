from google.cloud import storage

def create_bucket(bucket_name, bucket_location):
    """
    Create a new bucket based on input params
     * bucket name
     * bucket location
    with default storage class
    
    """

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    new_bucket = storage_client.create_bucket(bucket, location=bucket_location)

    print(
        "Created bucket {} in {}".format(
            new_bucket.name, new_bucket.location
        )
    )
    return new_bucket


def create_folder(bucket_name, destination_folder_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_folder_name)

    blob.upload_from_string('')

    print('Created {} .'.format(destination_folder_name))
    
# def doesFileExist(bucket, prefix) -> bool:
#     blobs = storage.list(bucket, prefix, BlobListOption.pageSize(1));
#     return blobs.getValues().iterator().hasNext();
}

# def create_folder(bucket_folder):
#       '''
#       reference: https://www.googlecloudcommunity.com/gc/Developer-Tools/How-to-create-a-Folder-in-the-GCP-Bucket-using-API/m-p/507047
#       '''
#     import requests

#     # Set the API endpoint URL
#     api_endpoint = "https://www.googleapis.com/storage/v1/b/{bucket}/o"

#     # Set the bucket name
#     bucket_name = "my-bucket"

#     # Set the folder name
#     folder_name = "my-folder"

#     # Set the API key
#     api_key = "YOUR_API_KEY"

#     # Set the request headers
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization": f"Bearer {api_key}"
#     }

#     # Set the request body
#     data = {
#         "name": folder_name,
#         "folder": True
#     }

#     # Send the POST request
#     response = requests.post(api_endpoint, headers=headers, json=data)

#     # Print the response status code
#     print(response.status_code)

