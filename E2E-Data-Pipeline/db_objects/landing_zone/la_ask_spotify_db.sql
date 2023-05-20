CREATE TABLE `la_artists` (
  `genre` varchar(360),
  `name` varchar(360),
  `id` varchar(360),
  `popularity` varchar(360),
  `followers` varchar(360),
  `data_load_id` int
);

CREATE TABLE `la_albums` (
  `name` varchar(360),
  `album_id` varchar(360),
  `release_date` varchar(360),
  `total_tracks` varchar(360),
  `artist_id` varchar(360),
  `data_load_id` int
);

CREATE TABLE `la_tracks` (
  `track_id` varchar(360),
  `track_name` varchar(360),
  `popularity` varchar(360),
  `duration_ms` varchar(360),
  `album_id` varchar(360),
  `artist_id` varchar(360),
  `data_load_id` int
);
