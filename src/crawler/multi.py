import ray
import boto3
from core import stream
from typing import List

def get_files_from_s3(bucket: str, file_prefix: str) -> List[boto3.resources.factory.s3.ObjectSummary]:
    """ Gets a list of object keys from S3 """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)

    s3_objs = bucket.objects.filter(Prefix=file_prefix).limit(10)
    return [obj for obj in s3_objs]

@ray.remote
def async_s3_file_processing(**kwargs):
    """ Ray remote function of s3_file_processing """
    return stream.s3_file_processing(**kwargs)

