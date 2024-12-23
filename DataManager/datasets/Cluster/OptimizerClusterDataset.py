import pandas as pd
import numpy as np

class OptimizerClusterDataset:
    def __init__(self):
        
        self.optimized_dtypes = {
            'song_id': 'category',
            'artis_name': 'category',
            'genre': 'category',
            'name': 'category',
            'duration_ms': np.int32,
            'explicit': bool,
            'release_date': 'category',
            'popularity': np.int8,
            'danceability': np.float32,
            'acousticness': np.float32,
            'energy': np.float32,
            'speechiness': np.float32,
            'valence': np.float32,
            'time_signature': np.int8,
            'mode': bool,
            'loudness': np.float32,
            'key': np.int8
        }

    def optimizer_dataset(self, df:pd.DataFrame) -> pd.DataFrame:
        """
        Ottimizza i valori del dataset
        """

        optimized_df = df.copy()

        for column, dtype in self.optimized_dtypes.items():
            if column in optimized_df.columns:
                optimized_df[column] = optimized_df[column].astype(dtype=dtype)
        
        numerical_feature = ['danceability', 'acousticness', 'energy', 
                            'speechiness', 'valence', 'loudness']

        return optimized_df