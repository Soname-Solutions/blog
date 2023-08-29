INSERT
	INTO
	tr_artists_genres (genre_id,
	artist_id,
    data_load_id)
SELECT
	DISTINCT
	MD5(genre) AS genre_id,
	MD5(id) AS artist_id,
    la.data_load_id
FROM
	la_artists la
WHERE la.data_load_id = %s