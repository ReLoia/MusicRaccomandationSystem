from dataclasses import dataclass

@dataclass
class ValidationError:
    field : str
    error_type: str
    message : str
    value : any
    expected : any