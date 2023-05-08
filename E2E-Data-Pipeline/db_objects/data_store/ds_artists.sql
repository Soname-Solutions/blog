create or replace table ds_artists(
    artist_id varchar(32),
    artist_src_id varchar(360),
    artist_nm varchar(360),
    artist_popularity int,
    artist_followers int,
    data_load_id int,
    primary key(artist_id)
);