import pandas as pd
import boto3
from botocore.exceptions import ClientError
from io import StringIO
from typing import Optional

def get_obj_file_size(bucket: str, key: str):
    """ 
        Gets the file size of an S3 object in bytes
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    filesize = 0
    try:
        obj = bucket.Object(key=key)
        filesize = obj.content_length
    except ClientError as err:
        print(str(err))
    return filesize

def stream_s3_file(bucket: str, key: str, file_size: int, sql_expression: Optional[str] = None, chunk_bytes: Optional[int] = 500000) -> str:
    """ 
        Streams an S3 file as a generator.
        
        Inputs
        ---
        bucket: str
            S3 Bucket where the object currently resides.
        key: str
            S3 object key
        file_size: int
            Total size of the S3 file to process
        sql_expression: Optional[str]
            S3 Select expression to use. Defaults to `SELECT * FROM S3Object`.
            See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html
        chunk_bytes: Optional[int]
            The chunk size to process per yield. Defaults to 500 KB or 0.5 MB.
        
        Returns
        ---
        file_results: str
            Contents of the file
    """
    sql_expression = sql_expression or "SELECT * FROM S3Object"
    s3_client = boto3.session.Session().client('s3')

    start_byte = 0
    end_byte = min(chunk_bytes, file_size)
    
    while start_byte < end_byte:
        response = s3_client.select_object_content(
            Bucket=bucket,
            Key=key,
            ExpressionType='SQL',
            Expression=sql_expression,
            InputSerialization={
                'CSV': {
                    'FileHeaderInfo': 'NONE',
                    'FieldDelimiter': ',',
                    'RecordDelimiter': '\n'
                }
            },
            OutputSerialization={
                'CSV': {
                    'RecordDelimiter': '\n'
                }
            },
            ScanRange={
                'Start': start_byte,
                'End': end_byte
            }
        )

        event_stream = response['Payload']
        results = []
        for event in event_stream:
            records = event.get('Records')
            if records:
                data = records['Payload']
                results.append(data.decode('utf-8'))
        yield ''.join(results)
        
        start_byte = end_byte
        end_byte = end_byte + min(chunk_bytes, file_size - end_byte)


def s3_file_processing(bucket: str, key: str, file_size_to_process: Optional[int] = 0, chunk_bytes: int = int(1e6)):
    """ 
        Process a single S3 file object by chunks in-memory.
        Streams an S3 file as a generator.
        
        Inputs
        ---
        bucket: str
            S3 Bucket where the object currently resides.
        key: str
            S3 object key
        file_size_to_process: Optional[int]
            How much of the file to process in bytes. Defaults to the entire file size.
        
        Returns
        ---
        None
    """

    file_size = get_obj_file_size(bucket, key) if file_size_to_process == 0 else file_size_to_process

    first_pass = True
    columns = []

    for chunk in stream_s3_file(bucket, key, file_size, chunk_bytes=chunk_bytes):
        chunk_input = StringIO(chunk)
        if first_pass:
            df = pd.read_csv(chunk_input)
            columns = df.columns
            first_pass = False
        else:
            df = pd.read_csv(chunk_input, header=None)
            df.columns = columns
        
        print(len(df))

        # perform processing here
        col_dtypes = df.dtypes.to_dict()
        dtypes = dict()

        for i, col_key in enumerate(col_dtypes.keys()):
            dtypes[col_key] = {
                'id': i, 
                'type': col_dtypes[col_key].name if not col_dtypes[col_key].name == 'object' else 'string'
            }

    
    return {'file_key': key, 'data': dtypes}



if __name__ == '__main__':
    # process only the first MB of the file contents
    s3_file_processing('123rf-data-lake', 'pixlr/users/2021/01/01/2021-01-01.csv', file_size_to_process=int(1e6))