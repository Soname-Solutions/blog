INSERT
	INTO
	tr_albums (album_id,
	artist_id,
	album_src_id,
	album_nm,
	release_dt,
	total_tracks,
	hdif,
	data_load_id)
SELECT
	DISTINCT
	MD5(al.album_id) AS album_id,
	COALESCE(ta.artist_id, -1) AS artist_id,
	al.album_id AS album_src_id,
	al.name AS album_nm, 
	DATE(CONCAT(al.release_date, '-01-01')) AS release_dt,
	CAST(al.total_tracks AS int) AS total_tracks,
	MD5(CONCAT( TRIM(al.artist_id), '|', TRIM(al.name), '|', TRIM(al.release_date), '|', TRIM(al.total_tracks), '|')) AS hdif,
	al.data_load_id
FROM
	la_albums al
LEFT JOIN (
	SELECT
			ta.artist_id,
			ta.artist_src_id
	FROM
			tr_artists ta
UNION
	SELECT
			da.artist_id,
			da.artist_src_id
	FROM
			ds_artists da
				) ta
				ON
	al.artist_id = ta.artist_src_id
WHERE
	al.data_load_id = %s