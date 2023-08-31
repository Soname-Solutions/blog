CREATE TABLE `tr_genres` (
  `genre_id` varchar(32),
  `genre_nm` varchar(360),
  `hdif` varchar(32),
  `data_load_id` int
);

CREATE TABLE `tr_artists_genres` (
  `genre_id` varchar(32),
  `artist_id` varchar(32),
  `hdif` varchar(32),
  `data_load_id` int
);

CREATE TABLE `tr_artists` (
  `artist_id` varchar(32),
  `artist_src_id` varchar(360),
  `artist_nm` varchar(360),
  `artist_popularity` int,
  `artist_followers` int,
  `hdif` varchar(32),
  `data_load_id` int
);

CREATE TABLE `tr_albums` (
  `album_id` varchar(32),
  `artist_id` varchar(32),
  `album_src_id` varchar(360),
  `album_nm` varchar(360),
  `release_dt` date,
  `total_tracks` int,
  `hdif` varchar(32),
  `data_load_id` int
);

CREATE TABLE `tr_tracks` (
  `track_id` varchar(32),
  `album_id` varchar(32),
  `artist_id` varchar(32),
  `track_src_id` varchar(360),
  `track_nm` varchar(360),
  `track_popularity` int,
  `duration_ms` int,
  `hdif` varchar(32),
  `data_load_id` int
);
