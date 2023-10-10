SET FOREIGN_KEY_CHECKS=0;

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
	incr_v_genres AS tr
WHERE
	ds_genre_id IS NULL
	AND tr.rn = 1;


UPDATE 
	ds_genres ds,
	(
	SELECT
		genre_id,
		genre_nm,
		hdif,
		data_load_id
	FROM
		incr_v_genres
	WHERE
		ds_genre_id IS NOT NULL
		AND rn = 1
		) tr_data
SET
	ds.genre_id = tr_data.genre_id,
	ds.genre_nm = tr_data.genre_nm,
	ds.hdif = tr_data.hdif,
	ds.data_load_id = tr_data.data_load_id
WHERE
	ds.genre_id = tr_data.genre_id
	AND ds.hdif != tr_data.hdif;

SET FOREIGN_KEY_CHECKS=1;
