
create or replace table ds_artists(
    artist_id varchar(32),
    artist_src_id varchar(360),
    artist_nm varchar(360),
    artist_popularity int,
    artist_followers int,
    data_load_id int,
    primary key(artist_id)
);

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

create or replace table ds_genres(
    genre_id varchar(32),
    genre_nm varchar(360),
    data_load_id int,
    primary key(genre_id)
);

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

create or replace table etl_control(
    data_load_id int auto_increment,
    file_nm varchar(360) not null,
    created_at timestamp default current_timestamp,
    primary key (data_load_id)
);

create or replace table la_artists(
    genre varchar(360),
    name varchar(360),
    id varchar(360),
    popularity varchar(360),
    followers varchar(360),
    data_load_id int
);

create or replace table la_albums(
    name varchar (360),
    album_id varchar (360),
    release_date varchar (360),
    total_tracks varchar(360),
    artist_id varchar(360),
    data_load_id int
);

create or replace table la_tracks(
    track_id varchar(360),
    track_name varchar(360),
    popularity varchar(360),
    duration_ms varchar(360),
    album_id varchar(360),
    artist_id varchar(360),
    data_load_id int
);

create or replace table tr_artists(
    artist_id varchar(32),
    artist_src_id varchar(360),
    artist_nm varchar(360),
    artist_popularity int,
    artist_followers int,
    data_load_id int
);

create or replace table tr_albums(
    album_id varchar (32),
    artist_id varchar(32),
    album_src_id varchar(360),
    album_nm varchar(360),
    release_dt date,
    total_tracks int,
    data_load_id int
);

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

create or replace table tr_genres(
    genre_id varchar(32),
    genre_nm varchar(360),
    data_load_id int
);

create or replace table tr_artists_genres(
    genre_id varchar(32),
    artist_id varchar(32),
    data_load_id int
);
