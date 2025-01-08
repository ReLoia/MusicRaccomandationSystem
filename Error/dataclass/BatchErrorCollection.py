from dataclass import dataclass, field
from typing import Dict, Optional, List
from datetime import datetime
from .BatchError import BatchError

import json

@dataclass
class BatchErrorCollection:
    total_batches : int = 0
    error : List[BatchError] = field(default_factory = list)
    erorrs_counts : Dict[str, int] = field(default_factory = list)
    start_time = datetime = field(default_factory = list)

    def add_error(self, error : BatchError) -> None:
        """
        Aggiunge un nuovo errore alla collezione 
        """
        self.error.append(error)
        self.erorrs_counts[error.error_type] = self.erorrs_counts.get(error.error_type, 0) + 1

    def get_error_summary(self) -> Dict:
        """
        Genera un riepilogo degli errori per il logging
        """

        return {
            'total_batches': self.total_batches,
            'total_errors': len(self.errors),
            'error_distribution': self.error_counts,
            'error_rate': len(self.errors) / self.total_batches if self.total_batches > 0 else 0,
            'errors_by_batch': {error.batch_index: error.to_dict() for error in self.errors},
            'processing_duration': (datetime.now() - self.start_time).total_seconds(),
        }

    def get_errors_by_type(self, error_type: str) -> List[BatchError]:
        """Recupera tutti gli errori di un determinato tipo"""
        return [error for error in self.errors if error.error_type == error_type]
    
    def get_errors_for_batch(self, batch_index: int) -> List[BatchError]:
        """Recupera tutti gli errori associati a un determinato batch"""
        return [error for error in self.errors if error.batch_index == batch_index]
    
    def to_json(self, file_path: str) -> None:
        """Salva la collezione di errori in un file JSON"""
        with open(file_path, 'w') as f:
            json.dump({
                'total_batches': self.total_batches,
                'total_errors': len(self.errors),
                'processing_duration': (datetime.now() - self.start_time).total_seconds(),
                'errors': [error.to_dict() for error in self.errors],
                'error_counts': self.error_counts,
                'critical_errors': len(self.get_critical_errors())
            }, f, indent=2)

    def clear_resolved_errors(self) -> None:
        """Rimuove gli errori che sono stati risolti con successo"""
        self.errors = [error for error in self.errors if error.is_critical()]
        self._update_error_counts()

    def _update_error_counts(self) -> None:
        """Aggiorna i conteggi degli errori"""
        self.error_counts = {}
        for error in self.errors:
            self.error_counts[error.error_type] = self.error_counts.get(error.error_type, 0)