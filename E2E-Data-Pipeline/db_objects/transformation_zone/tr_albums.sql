create or replace table tr_albums(
    album_id varchar (32),
    artist_id varchar(32),
    album_src_id varchar(360),
    album_nm varchar(360),
    release_dt date,
    total_tracks int,
    data_load_id int
);