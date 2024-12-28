import logging
import os
import threading

from dataclass.ProcessingResultBatch import ProcessingResults
from typing import List, Dict
from time import sleep
from BatchProcessingError import BatchProcessingError
from queue import Queue
from multiprocessing import Manager, cpu_count, Pool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self,max_retries, retry_delay):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.num_workers = cpu_count()
        self.manager = Manager()

        self.completed_batches = self.manager.Value('i', 0)
        self.failed_batches = self.manager.Value('i', 0)

        self.counter_lock = self.manager.Lock()

        self.batch_errors = BatchProcessingError()

    def process_batch_with_retry(self,
                                 batch_data : List[Dict],
                                 batch_index : int,
                                 process_func):
        
        """"
        Processa i batch con meccanismo di retry
        """

        worker_id = os.getpid()
        batch_index = batch_data['batch_index']
        data = batch_data['data']
        process_func = batch_data['process_func']

        attempt = 1
        last_error = None

        while attempt <= self.max_retries:
            try:
                logger.info(f'Tentativo {attempt} per batch {batch_index}')

                processed_data = process_func(batch_data)
                
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

        self.batch_errors.register_batch_error(batch_index, str(last_error))
        return ProcessingResults(
            success = False,
            data = [],
            error_type = str(last_error),
            batch_index = batch_index,
            attempt = attempt - 1,
            worker_id = worker_id
        )
    
    def process_all_batch(self, batches, process_func, start_from_batch: int = 0):
        self.batch_errors.set_total_batches(len(batches))
        results = []

        batch_data =[
            {
                'batch_index' : i+start_from_batch,
                'data' : batch,
                'process_func' : process_func
            }
            for i, batch in enumerate(batches[start_from_batch:])
        ]

        with Pool(processes=self.num_workers) as pool:
            results = []

            for result in pool.imap_unordered(self.process_batch_with_retry, batch_data):
                if result.success == True:
                    results.append(result)
        
        return sorted(results, key=lambda x: x.batch_index)

        return results