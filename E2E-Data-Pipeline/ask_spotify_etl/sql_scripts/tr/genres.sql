INSERT
	INTO
	tr_genres(genre_id,
	genre_nm,
	hdif,
    data_load_id)
SELECT
	DISTINCT
	MD5(la.genre) AS genre_id,
	la.genre AS genre_nm,
	MD5(CONCAT(TRIM(la.genre), '|')) hdif,
    la.data_load_id
FROM
	la_artists la
WHERE la.data_load_id = {data_load_id}