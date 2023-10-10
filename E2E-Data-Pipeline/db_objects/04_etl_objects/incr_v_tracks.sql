CREATE OR REPLACE
VIEW incr_v_tracks AS
	SELECT
		tr.track_id,
		tr.album_id,
		tr.artist_id,
		tr.track_src_id,
		tr.track_nm,
		tr.track_popularity,
		tr.duration_ms,
		tr.hdif,
		tr.data_load_id,
		ds.track_id AS ds_track_id,
		ROW_NUMBER() OVER(PARTITION BY tr.track_id
ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
FROM
		tr_tracks tr
LEFT JOIN ds_tracks ds ON
		tr.track_id = ds.track_id
INNER JOIN etl_control ec 
	ON
		tr.data_load_id = ec.data_load_id;
