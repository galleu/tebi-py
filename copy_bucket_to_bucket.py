# https://docs.tebi.io/s3/CopyObject.html

import requests, xmltodict, json
import _thread

# From Bucket
form_bucket_name = "form_bucket"
# Auth for the bucket, make sure it has list,copy permission
form_bucket_auth = "<Key>:<value>"

# To bucket
to_bucket_name = "to_bucket"
# Auth for the bucket, make sure it has write,copy permission
to_bucket_auth = "<Key>:<value>"


# The canned ACL to apply to the object. 
# Valid Values: private | public-read | public-read-write
acl = "public-read"
# Specifies whether the metadata is copied from the source object or replaced with metadata provided in the request. 
# Valid Values: COPY | REPLACE
metadata_copy = "COPY"

# Number of uploads at one time
upload_splits = 5


full_list = []
# List all objects in the bucket
def list_objects(marker):
    url = f'https://{form_bucket_name}/?max-keys=1000'
    if marker:
        url += f'&marker={marker}'
    print("Making Request to", url)
    response = requests.get(url, headers={'Authorization': f'TB-PLAIN {form_bucket_auth}'})
    o = xmltodict.parse(response.text)
    for row in o['ListBucketResult']['Contents']:
        full_list.append(row['Key'])

    if 'NextMarker' in o["ListBucketResult"]:
        list_objects(o["ListBucketResult"]["NextMarker"])
    else:
        dump = json.dumps(full_list, indent=4)
        # save to json to a file
        with open('list.json', 'w') as f:
            f.write(dump)



def copy(source, destination):
    print("Copying", source, "to", destination)
    headers = {
        'Authorization': f'TB-PLAIN {to_bucket_auth}',
        'x-amz-acl': acl,
        'x-amz-copy-source': source,
        'x-amz-metadata-directive': metadata_copy
    }
    response = requests.put("https://"+destination, headers=headers)
    print(response)


def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))




def download_chunk(c):
    print("Starting", len(c))
    for j in c:
        copy(f"{form_bucket_name}/{j}", f"{to_bucket_name}/{j}")


list_objects(False)

# open list.json
with open('list.json', 'r') as f:
    data = json.load(f)
    for i in split(data, upload_splits):
        _thread.start_new_thread(download_chunk, (i,))


    while True:
        pass
