SET FOREIGN_KEY_CHECKS=0;

INSERT
	INTO
	ds_artists (artist_id,
	artist_src_id,
	artist_nm,
	artist_popularity,
	artist_followers,
	hdif,
	data_load_id)
WITH CTE AS (
	SELECT
		tr.artist_id,
		tr.artist_src_id,
		tr.artist_nm,
		tr.artist_popularity,
		tr.artist_followers,
		tr.hdif,
		tr.data_load_id,
		ROW_NUMBER() OVER(PARTITION BY tr.artist_id
	ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
	FROM
		tr_artists tr
	LEFT JOIN ds_artists ds
	ON
		tr.artist_id = ds.artist_id
	INNER JOIN etl_control ec 
	ON
		tr.data_load_id = ec.data_load_id
	WHERE
		ds.artist_id IS NULL
)
SELECT
	tr.artist_id,
	tr.artist_src_id,
	tr.artist_nm,
	tr.artist_popularity,
	tr.artist_followers,
	tr.hdif,
	tr.data_load_id
FROM
	CTE AS tr
WHERE
	tr.rn = 1;


UPDATE
	ds_artists ds,
	(
	SELECT
		tr.artist_id,
		tr.artist_src_id,
		tr.artist_nm,
		tr.artist_popularity,
		tr.artist_followers,
		tr.hdif,
		tr.data_load_id,
		ROW_NUMBER() OVER(PARTITION BY tr.artist_id
	ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
	FROM
		tr_artists tr
	LEFT JOIN ds_artists ds
		ON
		tr.artist_id = ds.artist_id
	INNER JOIN etl_control ec 
		ON
		tr.data_load_id = ec.data_load_id
	WHERE
		ds.artist_id IS NOT NULL
	) tr_data
SET 
	ds.artist_id = tr_data.artist_id,
	ds.artist_src_id = tr_data.artist_src_id,
	ds.artist_nm = tr_data.artist_nm,
	ds.artist_popularity = tr_data.artist_popularity,
	ds.artist_followers = tr_data.artist_followers,
	ds.hdif = tr_data.hdif,
	ds.data_load_id = tr_data.data_load_id
WHERE
	ds.artist_id = tr_data.artist_id
	AND ds.hdif != tr_data.hdif
	AND tr_data.rn = 1;

SET FOREIGN_KEY_CHECKS=1;
