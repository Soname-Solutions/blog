INSERT
	INTO
	tr_albums (album_id,
	artist_id,
	album_src_id,
	album_nm,
	release_dt,
	total_tracks)
SELECT
	DISTINCT
	MD5(album_id) AS album_id,
	ta.artist_id AS artist_id,
	album_id AS album_src_id,
	name AS album_nm, 
	DATE(CONCAT(release_date, '-01-01')) AS release_dt,
	CAST(total_tracks AS int) AS total_tracks
FROM
	la_albums al
LEFT JOIN tr_artists ta ON
	al.artist_id = ta.artist_src_id;