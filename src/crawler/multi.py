import ray
import boto3
from core import stream

# 1. get files from bucket to process -- get dtypes
# 2. create tasks and assign files
# 3. execute and store returned metadata
# 4. store metadata to file on callback

def get_files_from_s3(bucket: str, file_prefix: str):
    """ Gets a list of object keys from S3 """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)

    s3_objs = bucket.objects.filter(Prefix=file_prefix).limit(10)
    return [obj.key for obj in s3_objs]

@ray.remote
def async_s3_file_processing(**kwargs):
    """ Ray remote function of s3_file_processing """
    return stream.s3_file_processing(**kwargs)

