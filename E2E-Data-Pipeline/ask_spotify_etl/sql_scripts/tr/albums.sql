INSERT
	INTO
	tr_albums (album_id,
	artist_id,
	album_src_id,
	album_nm,
	release_dt,
	total_tracks,
	data_load_id)
SELECT
	DISTINCT
	MD5(al.album_id) AS album_id,
	COALESCE(ta.artist_id, -1) AS artist_id,
	al.album_id AS album_src_id,
	al.name AS album_nm, 
	DATE(CONCAT(al.release_date, '-01-01')) AS release_dt,
	CAST(al.total_tracks AS int) AS total_tracks,
	%s
FROM
	la_albums al
LEFT JOIN tr_artists ta ON
	al.artist_id = ta.artist_src_id
where al.data_load_id = %s