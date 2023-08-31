INSERT
	INTO
	ds_genres 
	(
	genre_id,
	genre_nm,
	hdif,
	data_load_id)
SELECT
	tr.genre_id,
	tr.genre_nm,
	tr.hdif,
	tr.data_load_id
FROM
	tr_genres tr
LEFT JOIN ds_genres ds ON
	ds.genre_id = tr.genre_id
WHERE
	ds.genre_id IS NULL;


UPDATE 
	ds_genres ds,
	(
	SELECT
		tr.genre_id,
		tr.genre_nm,
		tr.hdif,
		tr.data_load_id
	FROM
		tr_genres tr
	LEFT JOIN ds_genres ds ON
		ds.genre_id = tr.genre_id
	WHERE
		ds.genre_id IS NOT NULL ) tr_data
SET
	ds.genre_id = tr_data.genre_id,
	ds.genre_nm = tr_data.genre_nm,
	ds.hdif = tr_data.hdif,
	ds.data_load_id = tr_data.data_load_id
WHERE
	ds.genre_id = tr_data.genre_id
	AND ds.hdif != tr_data.hdif;
