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
	inc_v_albums AS tr
WHERE
	ds_album_id IS NULL
	AND tr.rn = 1;

UPDATE
	ds_albums ds,
		(
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
		inc_v_albums AS tr
	WHERE
		ds_album_id IS NOT NULL
		AND tr.rn = 1
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
	AND ds.hdif != tr_data.hdif;

SET FOREIGN_KEY_CHECKS=1;
