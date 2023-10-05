INSERT
	INTO
	ds_artists_genres (genre_id,
	artist_id,
	hdif,
	data_load_id)
WITH CTE AS (
	SELECT
		tr.genre_id,
		tr.artist_id,
		tr.hdif,
		tr.data_load_id,
		ROW_NUMBER() OVER(PARTITION BY tr.genre_id,
		tr.artist_id
	ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
	FROM
		tr_artists_genres tr
	LEFT JOIN ds_artists_genres ds
		ON
		tr.hdif = ds.hdif
	INNER JOIN etl_control ec 
	ON
		tr.data_load_id = ec.data_load_id
	WHERE
		ds.hdif IS NULL)
SELECT 
	tr.genre_id,
	tr.artist_id,
	tr.hdif,
	tr.data_load_id
FROM
	CTE AS tr
WHERE
	tr.rn = 1;
