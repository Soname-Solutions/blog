create or replace table ds_albums(
    album_id varchar (32),
    artist_id varchar(32),
    album_src_id varchar(360),
    album_nm varchar(360),
    release_dt date,
    total_tracks int,
    data_load_id int,
    primary key(album_id),
    foreign key(artist_id)
        references ds_artists(artist_id)
);