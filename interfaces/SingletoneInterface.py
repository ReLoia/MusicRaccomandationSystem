from abc import ABC, abstractmethod

class SingletoneInterface(ABC):
    @classmethod
    @abstractmethod
    def get_instance(cls):
        pass

    def initialize(self):
        pass
    