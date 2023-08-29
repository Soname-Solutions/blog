INSERT
	INTO
	tr_artists (artist_id,
	artist_src_id,
	artist_nm,
	artist_popularity,
	artist_followers,
    data_load_id)
SELECT
	DISTINCT 
	MD5(la.id) AS artist_id,
	la.id AS artist_src_id,
	la.name AS artist_nm,
	CAST(la.popularity AS int) AS artist_popularity,
	CAST(la.followers AS int) AS artist_followers,
    la.data_load_id
FROM
	la_artists la
WHERE la.data_load_id = %s