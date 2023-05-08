create or replace table ds_artists_genres(
    genre_id varchar(32),
    artist_id varchar(32),
    data_load_id int,
    primary key(genre_id, artist_id),
    foreign key(artist_id)
        references ds_artists(artist_id),
    foreign key(genre_id)
        references ds_genres(genre_id)
);