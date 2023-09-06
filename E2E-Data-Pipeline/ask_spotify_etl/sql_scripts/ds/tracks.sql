SET FOREIGN_KEY_CHECKS=0;

INSERT
	INTO
	ds_tracks (track_id,
	album_id,
	artist_id,
	track_src_id,
	track_nm,
	track_popularity,
	duration_ms,
	hdif,
	data_load_id)
WITH CTE AS (
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
		ROW_NUMBER() OVER(PARTITION BY tr.track_id
	ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
	FROM
		tr_tracks tr
	LEFT JOIN ds_tracks ds ON
		tr.track_id = ds.track_id
	INNER JOIN etl_control ec 
	ON
		tr.data_load_id = ec.data_load_id
	WHERE
		ds.track_id IS NULL)
SELECT 
	tr.track_id,
	tr.album_id,
	tr.artist_id,
	tr.track_src_id,
	tr.track_nm,
	tr.track_popularity,
	tr.duration_ms,
	tr.hdif,
	tr.data_load_id
FROM
	CTE AS tr
WHERE
	tr.rn = 1;
	

UPDATE
	ds_tracks ds,
	(
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
		ROW_NUMBER() OVER(PARTITION BY tr.track_id
	ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
	FROM
		tr_tracks tr
	LEFT JOIN ds_tracks ds ON
		tr.track_id = ds.track_id
	INNER JOIN etl_control ec 
		ON
		tr.data_load_id = ec.data_load_id
	WHERE
		ds.track_id IS NOT NULL 
	) tr_data
SET
	ds.track_id = tr_data.track_id,
	ds.album_id = tr_data.album_id,
	ds.artist_id = tr_data.artist_id,
	ds.track_src_id = tr_data.track_src_id,
	ds.track_nm = tr_data.track_nm,
	ds.track_popularity = tr_data.track_popularity,
	ds.duration_ms = tr_data.duration_ms,
	ds.hdif = tr_data.hdif,
	ds.data_load_id = tr_data.data_load_id
WHERE 
	ds.track_id = tr_data.track_id
	AND ds.hdif != tr_data.hdif
	AND tr_data.rn = 1;

SET FOREIGN_KEY_CHECKS=1;
