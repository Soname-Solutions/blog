import os

import spotipy
import pandas as pd

from datetime import datetime
from abc import ABC, abstractmethod
from csv import QUOTE_ALL
from spotipy.oauth2 import SpotifyOAuth



class AskSpotifyParent(ABC):
    """abstract base class to inherit"""

    scope = "user-library-read, user-read-playback-state, user-follow-read"
    
    def __init__(self):
        pass

    @abstractmethod
    def get_data_raw(self):
        """submitting API request and getting returned data in JSON format. 
        considered as raw data"""
        pass
    
    @abstractmethod
    def get_data_df(self):
        """transform raw data into Pandas DataFrame"""
        pass
    

    def get_sp(self):
        """authentication method into Spotify API"""

        return spotipy.Spotify(auth_manager=SpotifyOAuth(scope=self.scope))
    

    def write_csv(self):
        """csv writing methond that is common for all Spotify data classes"""

        raw_data = self.get_data_raw()
        df_data = self.get_data_df(raw_data)
        df_data.to_csv(  path_or_buf = self.csv_save_path,
                                    sep = '#',
                                    na_rep = "",
                                    index=  False,
                                    quoting = QUOTE_ALL
                                )


class ArtistData(AskSpotifyParent):

    def __init__(self, landing_zone_path, date_format):
        self.sp = self.get_sp()
        self.csv_save_path = os.path.join(landing_zone_path, f"artists_{datetime.today().strftime(date_format)}.csv")


    def get_data_raw(self):
        """raw response from Spotify"""
        
        response = self.sp.current_user_followed_artists(limit=50)
        self.raw_data = response

        return response
    
    def get_followed_artists_ids(self):
        """prepare the list of artist_ids for later usage in Albums and Tracks"""

        followed_artists_id = []

        for i in range(len(self.raw_data['artists']['items'])):
            followed_artists_id.append(self.raw_data['artists']['items'][i]['id'])

        return followed_artists_id


    def get_data_df(self, raw_data):
        """ transform raw JSON data into Pandas dataframe """

        followed_artist_df =  pd.json_normalize(data = raw_data['artists']['items'], 
                                                record_path = ['genres'], 
                                                meta = ['name', 'id', 'popularity', ['followers' , 'total']])
        followed_artist_df.rename(columns = {0: 'genre', 'followers.total' : 'followers'}, inplace=True)

        return followed_artist_df


class ArtistAlbums(AskSpotifyParent):

    def __init__(self, landing_zone_path, date_format, artist_id_list):

        self.sp = self.get_sp()
        self.csv_save_path = os.path.join(landing_zone_path, f"albums_{datetime.today().strftime(date_format)}.csv")
        self.artist_id_list = artist_id_list
    

    def get_data_raw(self):

        albums_raw = []
        
        for artist_id in self.artist_id_list:

            results  = self.sp.artist_albums(   artist_id = artist_id, 
                                                album_type = 'album',
                                            )
        
            albums_raw.extend(results['items'])
            while results['next']:
                results = self.sp.next(results)
                albums_raw.extend(results['items'])
        
        return albums_raw
    

    def get_data_df(self, raw_data):

        albums_list = list()

        for raw_data_item in raw_data:
            
            raw_data_item_list = []
            # get only needed data into list
            raw_data_item_list.append(raw_data_item['name'])
            raw_data_item_list.append(raw_data_item['id'])
            raw_data_item_list.append(raw_data_item['release_date'])
            raw_data_item_list.append(raw_data_item['total_tracks'])
            raw_data_item_list.append(raw_data_item['artists'][0]['id'])

            albums_list.append(raw_data_item_list)


        albums_df = pd.DataFrame(data = albums_list,
                                columns = ['name', 'album_id', 'release_date', 'total_tracks',  'artist_id']
                                )
        return albums_df

class ArtistTracks(AskSpotifyParent):

    def __init__(self, landing_zone_path, date_format, artist_id_list):
        self.sp = self.get_sp()
        self.csv_save_path = os.path.join(landing_zone_path, f"tracks_{datetime.today().strftime(date_format)}.csv")
        self.artist_id_list = artist_id_list
    

    def get_data_raw(self):
        
        tracks_raw = []

        for artist_id in self.artist_id_list:
            result = self.sp.artist_top_tracks(artist_id=artist_id, country='DE')
            for track in result['tracks']:
                tracks_raw.append(track)
        
        return tracks_raw


    def get_data_df(self, raw_data):
        
        track_list = list()

        for raw_data_item in raw_data:

            raw_data_item_list = []
            raw_data_item_list.append(raw_data_item['id'])
            raw_data_item_list.append(raw_data_item['name'])
            raw_data_item_list.append(raw_data_item['popularity'])
            raw_data_item_list.append(raw_data_item['duration_ms'])
            raw_data_item_list.append(raw_data_item['album']['id'])
            raw_data_item_list.append(raw_data_item['artists'][0]['id'])

            track_list.append(raw_data_item_list)

        tracks_df = pd.DataFrame(data = track_list,
                                columns = [ 'track_id', 
                                            'track_name', 
                                            'popularity', 
                                            'duration_ms',  
                                            'album_id', 
                                            'artist_id']
                                )
        
        return tracks_df


if __name__ == '__main__':

    from dotenv import load_dotenv
    load_dotenv()
    landing_zone_path = os.path.join(os.path.curdir, 'landing_zone')
    date_format = '%Y_%m_%d'

    # Aritst data
    Artist = ArtistData(landing_zone_path, date_format)
    Artist.write_csv()

    # Albums data
    artist_id_list = ['0C0XlULifJtAgn6ZNCW2eu', '0TMi4dfaeWLOtRybyX09XW']

    Albums = ArtistAlbums(landing_zone_path, date_format, artist_id_list)
    Albums.write_csv()

    # Tracks data
    artist_id_list = [  '0C0XlULifJtAgn6ZNCW2eu', 
                    '0TMi4dfaeWLOtRybyX09XW',
                    '6XyY86QOPPrYVGvF9ch6wz'
                    ]
    Tracks = ArtistTracks(landing_zone_path, date_format, artist_id_list)
    Tracks.write_csv()