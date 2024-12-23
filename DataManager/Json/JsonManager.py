import pandas as pd
import numpy as np

from JsonValidator import JsonValidator
from typing import List, Dict, Optional
from multiprocessing import Pool, cpu_count


class JsonManager:
    def __init__(self, batch_size):
        self.json_validator = JsonValidator()
        self.batch_size = batch_size
        self.dataframe : Optional[pd.DateFrame] = None

    def process_song_data(self, songs_data : List[Dict]) -> None:
        
        """
        Struttura le canzoni in batch e per ogni batch esegue il metodo process_song_batch
        in parallelo.
        Converte tutti i batch processati in un Daframe di pandas e richiama la 
        """

        batches = [songs_data[i:i+self.batch_size]
                   for i in range(0, len(songs_data), self.batch_size)]
        
        with Pool(processes=cpu_count()) as pool:
            processed_batches = pool.map(self.process_song_batch, batches)
        
        all_processed_songs = [song for batch in processed_batches for song in batch]

        self.dataframe = pd.DateFrame(all_processed_songs)

    def process_batch_song(self, batch_song : List[Dict]) -> List[Dict]:
        """
        Processa un singolo batch di canzoni, rende la struttura json
        una struttura tabellare, più ottimizzata per i dataframe di pandas
        """

        songs_data = []

        for song in batch_song:
            
            song_validate_status = self.json_validator(song)

            if song_validate_status == True:

                processed_song = {
                    'son_id' : song['id'],
                    'artis_name' : song['artist_name'],
                    'genre' : song['genre'],
                    'name' : song['name'],
                    'duration_ms' : song['duration_ms'],
                    'explicit' : 1 if song['explicit'] == True else 0,
                    'release_date' : song['release_date'],
                    'popularity' : song['popularity'],
                    'danceability' : song['danceability'],
                    'acousticness' : song['acousticness'],
                    'energy' : song['energy'],
                    'speechiness' : song['speechiness'],
                    'valence' : song['valence'],
                    'time_signature' : song['time_signature'],
                    'mode' : song['mode'],
                    'loudness' : song['loudness'],
                    'key' : song['key'],
                }

                songs_data.append(processed_song)

        return songs_data


    def process_user_preferences(self, user_json):
        """"
        Processa lo storico degli ascolti di un utente e crea un profilo
        e crea un profilo dell'utente
        
        """ 

        user_history = {
            'user_id' : user_json['id'],
            'listening_song' : [self.process_song_data(song) for song in user_json['history']]
        }
    
        return user_history
    
    def get_dataset(self):
        return self.dataframe