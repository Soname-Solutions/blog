CREATE OR REPLACE
VIEW inc_v_albums AS 
	SELECT
		tr.album_id,
		tr.artist_id,
		tr.album_src_id,
		tr.album_nm,
		tr.release_dt,
		tr.total_tracks,
		tr.hdif,
		tr.data_load_id,
		ds.album_id AS ds_album_id,
		ROW_NUMBER() OVER(PARTITION BY tr.album_id
ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
FROM
		tr_albums tr
LEFT JOIN ds_albums ds ON
		tr.album_id = ds.album_id
INNER JOIN etl_control ec 
	ON
		tr.data_load_id = ec.data_load_id;
