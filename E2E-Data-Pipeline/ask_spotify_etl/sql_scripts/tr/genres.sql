INSERT
	INTO
	tr_genres(genre_id,
	genre_nm,
    data_load_id)
SELECT
	DISTINCT
	MD5(la.genre) AS genre_id,
	la.genre AS genre_nm,
    %s
FROM
	la_artists la
WHERE la.data_load_id = %s