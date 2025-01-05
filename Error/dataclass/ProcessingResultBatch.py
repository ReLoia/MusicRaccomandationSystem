from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass
class ProcessingResults:
    sucess : bool
    data : List[Dict]
    error_type : Optional[str]
    batch_index : int
    attempt : int
    worker_id : int
