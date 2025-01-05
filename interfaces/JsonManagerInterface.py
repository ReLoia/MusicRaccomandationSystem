from .SingletoneInterface import SingletoneInterface
from abc import abstractmethod
from typing import List, Dict

class JsonManagerInterface(SingletoneInterface):
    @classmethod
    @abstractmethod
    def process_song_data(self, songs_data : List[Dict]) -> None:
        pass
    
    @abstractmethod
    def process_batch(self, batch_data):
        pass

    @abstractmethod
    def process_single_song(self, song : List[Dict]) -> List[Dict]:
        pass

    @abstractmethod
    def initialize(self, batch_size,):
        pass
