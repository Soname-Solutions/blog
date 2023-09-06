SET FOREIGN_KEY_CHECKS=0;

INSERT
	INTO
	ds_genres 
	(
	genre_id,
	genre_nm,
	hdif,
	data_load_id)
WITH CTE AS (
	SELECT
		tr.genre_id,
		tr.genre_nm,
		tr.hdif,
		tr.data_load_id,
		ROW_NUMBER() OVER(PARTITION BY tr.genre_id
	ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
	FROM
		tr_genres tr
	LEFT JOIN ds_genres ds ON
		ds.genre_id = tr.genre_id
	INNER JOIN etl_control ec 
	ON
		tr.data_load_id = ec.data_load_id
	WHERE
		ds.genre_id IS NULL)
SELECT
	tr.genre_id,
	tr.genre_nm,
	tr.hdif,
	tr.data_load_id
FROM
	CTE AS tr
WHERE
	tr.rn = 1;


UPDATE 
	ds_genres ds,
	(
	SELECT
		tr.genre_id,
		tr.genre_nm,
		tr.hdif,
		tr.data_load_id,
		ROW_NUMBER() OVER(PARTITION BY tr.genre_id
	ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
	FROM
		tr_genres tr
	LEFT JOIN ds_genres ds ON
		ds.genre_id = tr.genre_id
	INNER JOIN etl_control ec 
		ON
		tr.data_load_id = ec.data_load_id
	WHERE
		ds.genre_id IS NOT NULL ) tr_data
SET
	ds.genre_id = tr_data.genre_id,
	ds.genre_nm = tr_data.genre_nm,
	ds.hdif = tr_data.hdif,
	ds.data_load_id = tr_data.data_load_id
WHERE
	ds.genre_id = tr_data.genre_id
	AND ds.hdif != tr_data.hdif
	AND tr_data.rn = 1;

SET FOREIGN_KEY_CHECKS=1;
