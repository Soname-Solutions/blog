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
	hdif,
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
	MD5(CONCAT(TRIM(COALESCE(ta.artist_id, -1)), '|', TRIM(COALESCE(al.album_id, -1)), '|', TRIM(la.track_name), '|', TRIM(la.popularity), '|', TRIM(la.duration_ms), '|')) AS hdif,
	la.data_load_id
FROM
	la_tracks la
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
		la.artist_id = ta.artist_src_id
LEFT JOIN (
		SELECT
				ta.album_id,
				ta.album_src_id
		FROM
				tr_albums ta
	UNION
		SELECT
				da.album_id,
				da.album_src_id
		FROM
				ds_albums da
	) al 
ON
		la.album_id = al.album_src_id
WHERE
	la.data_load_id = {data_load_id}