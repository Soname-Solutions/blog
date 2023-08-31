CREATE TABLE `ds_genres` (
  `genre_id` varchar(32) PRIMARY KEY,
  `genre_nm` varchar(360),
  `hdif` varchar(32),
  `data_load_id` int
);

CREATE TABLE `ds_artists_genres` (
  `genre_id` varchar(32),
  `artist_id` varchar(32),
  `hdif` varchar(32),
  `data_load_id` int,
  PRIMARY KEY (`genre_id`, `artist_id`)
);

CREATE TABLE `ds_artists` (
  `artist_id` varchar(32) PRIMARY KEY,
  `artist_src_id` varchar(360),
  `artist_nm` varchar(360),
  `artist_popularity` int,
  `artist_followers` int,
  `hdif` varchar(32),
  `data_load_id` int
);

CREATE TABLE `ds_albums` (
  `album_id` varchar(32) PRIMARY KEY,
  `artist_id` varchar(32),
  `album_src_id` varchar(360),
  `album_nm` varchar(360),
  `release_dt` date,
  `total_tracks` int,
  `hdif` varchar(32),
  `data_load_id` int
);

CREATE TABLE `ds_tracks` (
  `track_id` varchar(32) PRIMARY KEY,
  `album_id` varchar(32),
  `artist_id` varchar(32),
  `track_src_id` varchar(360),
  `track_nm` varchar(360),
  `track_popularity` int,
  `duration_ms` int,
  `hdif` varchar(32),
  `data_load_id` int
);

ALTER TABLE `ds_artists_genres` ADD FOREIGN KEY (`genre_id`) REFERENCES `ds_genres` (`genre_id`);

ALTER TABLE `ds_artists_genres` ADD FOREIGN KEY (`artist_id`) REFERENCES `ds_artists` (`artist_id`);

ALTER TABLE `ds_albums` ADD FOREIGN KEY (`artist_id`) REFERENCES `ds_artists` (`artist_id`);

ALTER TABLE `ds_tracks` ADD FOREIGN KEY (`album_id`) REFERENCES `ds_albums` (`album_id`);

ALTER TABLE `ds_tracks` ADD FOREIGN KEY (`artist_id`) REFERENCES `ds_artists` (`artist_id`);
