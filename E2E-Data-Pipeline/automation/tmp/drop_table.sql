ALTER TABLE ds_albums DROP FOREIGN KEY ds_albums_ibfk_1; 
ALTER TABLE ds_artists_genres DROP FOREIGN KEY ds_artists_genres_ibfk_1; 
ALTER TABLE ds_artists_genres DROP FOREIGN KEY ds_artists_genres_ibfk_2; 
ALTER TABLE ds_tracks DROP FOREIGN KEY ds_tracks_ibfk_1; 
ALTER TABLE ds_tracks DROP FOREIGN KEY ds_tracks_ibfk_2; 
DROP TABLE IF EXISTS ds_genres;
DROP TABLE IF EXISTS ds_artists_genres;
DROP TABLE IF EXISTS ds_artists;
DROP TABLE IF EXISTS ds_albums;
DROP TABLE IF EXISTS ds_tracks;
DROP TABLE IF EXISTS etl_control;
DROP TABLE IF EXISTS la_artists;
DROP TABLE IF EXISTS la_albums;
DROP TABLE IF EXISTS la_tracks;
DROP TABLE IF EXISTS tr_genres;
DROP TABLE IF EXISTS tr_artists_genres;
DROP TABLE IF EXISTS tr_artists;
DROP TABLE IF EXISTS tr_albums;
DROP TABLE IF EXISTS tr_tracks;
