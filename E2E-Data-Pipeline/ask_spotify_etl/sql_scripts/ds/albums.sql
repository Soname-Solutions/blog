SET FOREIGN_KEY_CHECKS=0;

INSERT
	INTO
	ds_albums (album_id,
	artist_id,
	album_src_id,
	album_nm,
	release_dt,
	total_tracks,
	hdif,
	data_load_id)
WITH CTE AS (
	SELECT
		tr.album_id,
		tr.artist_id,
		tr.album_src_id,
		tr.album_nm,
		tr.release_dt,
		tr.total_tracks,
		tr.hdif,
		tr.data_load_id,
		ROW_NUMBER() OVER(PARTITION BY tr.album_id
	ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
	FROM
		tr_albums tr
	LEFT JOIN ds_albums ds ON
		tr.album_id = ds.album_id
	INNER JOIN etl_control ec 
	ON
		tr.data_load_id = ec.data_load_id
	WHERE
		ds.album_id IS NULL
)
SELECT 
	tr.album_id,
	tr.artist_id,
	tr.album_src_id,
	tr.album_nm,
	tr.release_dt,
	tr.total_tracks,
	tr.hdif,
	tr.data_load_id
FROM
	CTE AS tr
WHERE
	tr.rn = 1;

UPDATE
	ds_albums ds,
		(
	SELECT
		tr.album_id,
		tr.artist_id,
		tr.album_nm,
		tr.release_dt,
		tr.total_tracks,
		tr.hdif,
		tr.data_load_id,
		ROW_NUMBER() OVER(PARTITION BY tr.album_id
	ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
	FROM
		tr_albums tr
	LEFT JOIN ds_albums ds ON
		tr.album_id = ds.album_id
	INNER JOIN etl_control ec 
		ON
		tr.data_load_id = ec.data_load_id
	WHERE
		ds.album_id IS NOT NULL
) tr_data
SET
	ds.artist_id = tr_data.artist_id,
	ds.album_nm = tr_data.album_nm,
	ds.release_dt = tr_data.release_dt,
	ds.total_tracks = tr_data.total_tracks,
	ds.hdif = tr_data.hdif,
	ds.data_load_id = tr_data.data_load_id
WHERE
	ds.album_id = tr_data.album_id
	AND ds.hdif != tr_data.hdif
	AND tr_data.rn = 1;

SET FOREIGN_KEY_CHECKS=1;
