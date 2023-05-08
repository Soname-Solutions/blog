create or replace table tr_tracks(
    track_id varchar(32),
    artist_id varchar(32),
    album_id varchar(32),
    track_src_id varchar(360),
    track_nm varchar(360),
    track_popularity int,
    duration_ms int,
    data_load_id int
);