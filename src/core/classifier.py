import boto3
import pandas as pd
from botocore.exceptions import ClientError
from io import StringIO
from typing import Optional, Dict

class S3BaseClassifier:
    """ Base class for all S3 Select object classifiers """
    def __init__(self, 
                bucket: str,
                input_serialization: Dict,
                output_serialization: Dict,
                expression_type: str = 'SQL',
                query: str = None):
        
        self.bucket = bucket
        query = query if query else "SELECT * FROM S3Object"

        self._s3_select_object_content_dict = {
            'Bucket': bucket,
            'ExpressionType': expression_type,
            'Expression': query,
            'InputSerialization': input_serialization,
            'OutputSerialization': output_serialization
        }


class CSVClassifier(S3BaseClassifier):
    """ 
        CSV Classifier for S3 file objects. Returns metadata information
        for a given S3 object key.
    """
    def __init__(self, bucket: str = '', separator: str = ',', delimiter: str = '\n'):
        _input_serialization = {
            'CSV': {
                'FileHeaderInfo': 'NONE',
                'FieldDelimiter': separator,
                'RecordDelimiter': delimiter
            }
        }
        _output_serialization = {
            'CSV': {
                'RecordDelimiter': '\n'
            }
        }

        super().__init__(
            bucket=bucket, 
            input_serialization=_input_serialization, 
            output_serialization=_output_serialization
        )
        
    def stream_csv(self, 
                key: str, 
                file_size: int, 
                sql_expression: Optional[str] = None, 
                chunk_bytes: Optional[int] = 500000) -> str:
        """ 
            Streams an S3 csv file as a generator.
            
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
        s3_client = boto3.session.Session().client('s3')

        start_byte = 0
        end_byte = min(chunk_bytes, file_size)
        
        # TODO: check what error is raised when select_object_content called on a JSON file
        while start_byte < end_byte:
            response = s3_client.select_object_content(
                Key=key,
                ScanRange={
                    'Start': start_byte,
                    'End': end_byte
                },
                **self._s3_select_object_content_dict,
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
        
    def classify_csv(self, 
                        key: str, 
                        file_size_to_process: Optional[int] = 0, 
                        chunk_bytes: int = int(1e6)):
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
            JSON serializable object containing object key and metadata
        """

        file_size = get_obj_file_size(self._bucket, key) if file_size_to_process == 0 else file_size_to_process

        first_pass = True
        columns = []

        for chunk in self.stream_csv(key, file_size, chunk_bytes=chunk_bytes):
            chunk_input = StringIO(chunk)
            if first_pass:
                df = pd.read_csv(chunk_input)
                columns = df.columns
                first_pass = False
            else:
                df = pd.read_csv(chunk_input, header=None)
                df.columns = columns
            
            # perform processing here
            col_dtypes = df.dtypes.to_dict()
            dtypes = dict()

            for i, col_key in enumerate(col_dtypes.keys()):
                dtypes[col_key] = {
                    'id': i, 
                    'type': col_dtypes[col_key].name if not col_dtypes[col_key].name == 'object' else 'string'
                }

        
        return {'file_key': key, 'metadata': dtypes}

class JSONClassifier(S3BaseClassifier):
    def __init__(self):
        _input_serialization = {
            'JSON': {
                'Type': 'Document'
            }
        }
        _output_serialization = {
            'CSV': {
                'RecordDelimiter': '\n'
            }
        }

        super().__init__(
            bucket=bucket, 
            input_serialization=_input_serialization, 
            output_serialization=_output_serialization
        )

    def stream_json(self):
        pass