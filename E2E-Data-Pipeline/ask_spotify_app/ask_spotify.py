import os
from get_spotify_data import ArtistData, ArtistAlbums, ArtistTracks
from dotenv import load_dotenv

# load API keys as env var
load_dotenv()

# target folder to save CSV files
landing_zone_path = os.path.join(os.path.curdir, 'landing_zone')
date_format = '%Y_%m_%d'



def main():

    Artist = ArtistData(landing_zone_path, date_format)
    Artist.write_csv()
    artist_id_list = Artist.get_followed_artists_ids()

    Albums = ArtistAlbums(landing_zone_path, date_format, artist_id_list)
    Albums.write_csv()
    
    Tracks = ArtistTracks(landing_zone_path, date_format, artist_id_list)
    Tracks.write_csv()


if __name__ == '__main__':
    main()