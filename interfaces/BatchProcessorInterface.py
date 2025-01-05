from .SingletoneInterface import SingletoneInterface
from abc import abstractmethod
from typing import List, Dict

class BatchProcessorInterface(SingletoneInterface):
    @abstractmethod
    def process_batch_with_retry(self, batch_data : List[Dict],):
        pass

    @abstractmethod
    def process_all_batch(self, batches, start_from_batch : int):
        pass