create or replace table ds_tracks(
    track_id varchar(32),
    artist_id varchar(32),
    album_id varchar(32),
    track_src_id varchar(360),
    track_nm varchar(360),
    track_popularity int,
    duration_ms int,
    data_load_id int,
    primary key (track_id),
    foreign key (artist_id)
        references ds_artists(artist_id),
    foreign key (album_id)
        references ds_albums(album_id)
);