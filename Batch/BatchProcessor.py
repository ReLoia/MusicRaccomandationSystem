import logging
import os
import threading
import os
import sys
import concurrent.futures

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)
from Error.dataclass.ProcessingResultBatch import ProcessingResults
#from BatchProcessingError import BatchProcessingError

from interfaces.BatchProcessorInterface import BatchProcessorInterface

from typing import List, Dict
from time import sleep
from queue import Queue
from multiprocessing import Manager, cpu_count
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchProcessor(BatchProcessorInterface):
    instance = None

    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(cls)
            cls.instance._initialize = False
        return cls.instance

    def __init__(self,max_retries, retry_delay, json_manager):
        if not self._initialize:
            self.initialize(max_retries, retry_delay, json_manager)
    
    def initialize(self, max_retries, retry_delay, json_manager):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.json_manager = json_manager

        self.num_workers = cpu_count()
        self.manager = Manager()

        self.completed_batches = self.manager.Value('i', 0)
        self.failed_batches = self.manager.Value('i', 0)

        self.counter_lock = self.manager.Lock()

        #self.batch_errors = BatchProcessingError()

        self._initialize = True

    @classmethod
    def get_instance(self, cls):
        return cls.instance


    def process_batch_with_retry(self, batch_data : List[Dict],):
        
        """"
        Processa i batch con meccanismo di retry
        """

        worker_id = os.getpid()
        batch_index = batch_data['batch_index']
        data = batch_data['data']

        attempt = 1
        last_error = None

        while attempt <= self.max_retries:
            try:
                logger.info(f'Tentativo {attempt} per batch {batch_index}')

                processed_data = self.json_manager.process_batch(data)
                
                if processed_data:

                    with self.counter_lock:
                        self.completed_batches.value += 1

                    return ProcessingResults(
                        success = True,
                        data = processed_data,
                        error_type = None,
                        batch_index = batch_index,
                        attempt = attempt,
                        worker_id = worker_id
                    )
            except Exception as e:
                last_error = str(e)
                logger.warning(f'Tentativo {attempt} fallito per batch {batch_index}: {last_error}')

                if attempt < self.max_retries:
                    sleep(self.retry_delay)

                else:
                    self.failed_batches.value += 1
            attempt += 1
        #self.batch_errors.register_batch_error(batch_index, str(last_error))
        return ProcessingResults(
            success = False,
            data = [],
            error_type = str(last_error),
            batch_index = batch_index,
            attempt = attempt - 1,
            worker_id = worker_id
        )
    

    def process_all_batch(self, batches, start_from_batch: int):
        #self.batch_errors.set_total_batches(len(batches))
        results = []

        batch_data =[
            {
                'batch_index' : i+start_from_batch,
                'data' : batch,
            }
            for i, batch in enumerate(batches[start_from_batch:])
        ]

        try:
            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                future_to_batch = {
                    executor.submit(self.process_batch_with_retry, batch) : batch
                    for batch in batch_data
                }

                for future in concurrent.futures.as_completed(future_to_batch):

                    try:
                        result = future.result()
                        if result.success:
                            logger.info(f'Batch {result.batch_index} processato con successo')
                            results.append(result)
                    except Exception as e:
                        logger.error(f'Errore di elaborazione batch: {str(e)}')
        except Exception as e:
            logging.error(f'Errore di elaborazione parallela dei batch: {str(e)}')