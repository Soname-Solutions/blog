INSERT
	INTO
	tr_tracks
 (track_id,
	artist_id,
	album_id,
	track_src_id,
	track_nm,
	track_popularity,
	duration_ms,
    data_load_id)
SELECT
	DISTINCT 
	MD5(la.track_id) AS track_id,
	COALESCE(ta.artist_id, -1) AS artist_id,
	COALESCE(al.album_id, -1) AS album_id,
	la.track_id AS track_src_id,
	la.track_name AS track_nm,
	CAST(la.popularity AS int) AS track_popularity,
	CAST(la.duration_ms AS int) AS duration_ms,
    %s
FROM
	la_tracks la
LEFT JOIN tr_artists ta ON
		la.artist_id = ta.artist_src_id
LEFT JOIN tr_albums al ON
		la.album_id = al.album_src_id
WHERE la.data_load_id = %s
	