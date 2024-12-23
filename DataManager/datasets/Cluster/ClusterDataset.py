import pandas as pd
import numpy as np

from Json.JsonManager import JsonManager

class ClusterDataset:
    def __init__(self, json_manager):
        self.dataset = json_manager.get_dataset()

    def optimize_dtypes_dataset(self) -> None:
        categorical_column = ['song_id', 'artist_name', 'genre', 'name', 'release_date']
