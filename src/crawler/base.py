import os
import requests
import logging
import logging.config
from typing import Optional, List, Any, Dict
from pathlib import Path
import json

import yaml
import ray
from ray.util import ActorPool

from crawler.multi import get_files_from_s3, async_s3_file_processing
from core.classifier import S3BaseClassifier, CSVClassifier, JSONClassifier

with open(os.path.join(os.path.dirname(__file__), '..', '..', 'config.yaml')) as f:
    logging.config.dictConfig(
        yaml.safe_load(f)['logging']
    )

logger = logging.getLogger('mainLogger')

@ray.remote
def process_s3_object(classifiers: List[S3BaseClassifier], key: str, file_size_to_process: int = int(1e6)):
    """ Functional implementation of CrawlerWorker """
    for classifier in classifiers:
        if isinstance(classifier, CSVClassifier):
            try:
                return classifier.classify_csv(key, file_size_to_process)
            except Exception as err:
                print(f'Failed to classify csv: {str(err)}')
    
        if isinstance(classifier, JSONClassifier):
            pass

class ElastiSearch:
    DEFAULT_ENDPOINT = 'http://localhost:9200/'

    def __init__(self, es_endpoint: str = None):
        self.endpoint = es_endpoint if es_endpoint else ElastiSearch.DEFAULT_ENDPOINT

    def add(self, index: str, doc):
        """
            Adds a doc to elastisearch.

            Inputs
            ---
            doc: JSON Serializable dictionary

            Outputs
            ---
            None
        """
        r = requests.post(self.endpoint + f'{index}/_doc', json=doc)
        assert r.status_code == 201
        return None

class S3Crawler:
    """ Crawls S3 files for metadata and schema """

    def __init__(self,
                elastisearch_endpoint: str = None, 
                json_output_path: str = None
            ):

        outputter = {
            'elastisearch_endpoint': elastisearch_endpoint,
            'json_output_path': json_output_path
        }
        
        self._output_handler = Outputter(**outputter)

    def crawl(self, bucket: str, prefix_to_search: str, num_workers: int = None, classifiers: List[S3BaseClassifier] = None):
        """ 
            Main driver function of the crawler. Initialises a number of Ray
            workers to run the classifiers with.
            
            Inputs
            ---
            bucket: str
            prefix_to_search: str
                Where the crawler will begin its search
            num_workers: int
                Number of Ray workers to instantiate
            classifiers: List[S3BaseClassifier]
                List of classifiers to extract the schema with
            
            Outputs
            ---
            None

        """
        if classifiers is None: classifiers = [CSVClassifier(bucket=bucket)]
        logger.info('Instantiating..')
        num_workers = 2 if num_workers is None else num_workers
        
        logger.info('Starting ray cluster..')
        ray.init(num_cpus=num_workers)

        logger.info('Fetching s3 objects..')
        objs = get_files_from_s3(bucket, file_prefix=prefix_to_search)

        logger.info('Instantiating worker pool..')
        pool = ActorPool([CrawlerWorker.remote(classifiers=classifiers) for _ in range(num_workers)])

        # returns a list of JSON serializable objects
        logger.info('Beginning to skrawl...')
        outputs = pool.map(lambda actor, obj: actor.extract_schema_from_s3_object.remote(key=obj.key), objs)
        outputs = list(outputs)

        self._output_handler.to_elastisearch(index='123rf-data-lake', docs=outputs)
        self._output_handler.to_json(outputs)
        
        logger.info('Skrawler done.')


class Outputter:
    """ Handles the output of the crawl metadata """

    def __init__(self, json_output_path: Optional[str] = None, elastisearch_endpoint: Optional[str] = None):
        """ 
            Inputs
            ---
            json_output_path: Optional[str]
                Filepath to write JSON metadata to.
            elastisearch_endpoint: Optional[str]
                Elastisearch endpoint to write metadata.
        """
        self._es        = None
        self._json_path = None

        if elastisearch_endpoint is not None and isinstance(elastisearch_endpoint, str):
            logger.info(f'Output metadata will be saved to Elastisearch: {elastisearch_endpoint}')
            self._es = ElastiSearch(es_endpoint=elastisearch_endpoint)

        if json_output_path is not None and isinstance(json_output_path, str):
            logger.info(f'Output metadata will be written to JSON: {json_output_path}')
            self._json_path = json_output_path

            # creates the parent/s dir if it does not exist
            pth = Path(self._json_path)
            pth.parent.mkdir(parents=True, exist_ok=True)
    
    def to_elastisearch(self, index: str, docs: List[Any]):
        """ 
            TODO: Update type to something better than List[Any]. Ditto save_to_json
            Inputs
            ---
            docs: list of JSON serializable objects
            
            Outputs
            ---
            None
        """
        if not isinstance(self._es, ElastiSearch):
            raise Exception('Elastisearch endpoint is not defined.')

        if not hasattr(docs, '__len__'):
            raise Exception('Items to be saved is not a list of JSON serializable objects.')
        
        if len(docs) == 0:
            raise Exception('No items to save.')
        
        for doc in docs:
            try:
                logger.info(f'Adding doc to elastisearch: {doc["file_key"]}')
                self._es.add(index=index, doc=doc)
            except Exception as err:
                print(f'Error saving to elastisearch: {str(err)}')

    def to_json(self, docs: List[Any]):
        """ 
            Inputs
            ---
            docs: list of JSON serializable objects
            
            Outputs
            ---
            None
        """
        if self._json_path is None:
            raise Exception('No output JSON path specified.')

        with open(self._json_path, 'w') as f:
            logger.info('Saving to JSON file.')
            f.write(json.dumps(docs))
        
@ray.remote(num_cpus=1)
class CrawlerWorker:
    """ 
        Worker used to run classifiers on S3 object keys.
    """
    def __init__(self, classifiers: List[S3BaseClassifier]):
        self._classifiers = classifiers

    @ray.method(num_returns=1)
    def extract_schema_from_s3_object(self, key: str, file_size_to_process: int = int(1e6)) -> Dict:
        """ 
            Driver function to extract schema metadata using the provided classifiers.
            The Worker will loop through each classifier -- if a given classifier fails
            to extract the metadata, the worker will move on to the next classifier
            specified in the list.

            Inputs
            ---
            key: str
                S3 object key
            file_size_to_process: int
                Total file size to sample in order to infer data types.
            
            Outputs
            ---
            output: dict
                Key-value mappings of the form: `{'file_key': key, 'metadata': dtypes}`,
                where the dtypes is a dictionary of the schema
        """
        for classifier in self._classifiers:

            if isinstance(classifier, CSVClassifier):
                try:
                    return classifier.classify_csv(key, file_size_to_process)
                except Exception as err:
                    logger.error(f'Failed to classify csv: {str(err)}')
                    logger.exception('extract_schema_from_s3_object')
        
            if isinstance(classifier, JSONClassifier):
                # TODO:
                pass

