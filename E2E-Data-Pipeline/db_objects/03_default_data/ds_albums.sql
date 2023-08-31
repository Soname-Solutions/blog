SET FOREIGN_KEY_CHECKS=0;
INSERT INTO ds_albums (album_id,artist_id,album_src_id,album_nm,release_dt,total_tracks, hdif, data_load_id)
VALUES(-1, -1, -1, -1, STR_TO_DATE( '0000-00-00', '%Y-%m-%d'), -1, -1, -1);
SET FOREIGN_KEY_CHECKS=1;
