CREATE OR REPLACE
VIEW incr_v_genres AS 
	SELECT
		tr.genre_id,
		tr.genre_nm,
		tr.hdif,
		tr.data_load_id,
		ds.genre_id AS ds_genre_id, 
		ROW_NUMBER() OVER(PARTITION BY tr.genre_id
ORDER BY
		STR_TO_DATE(SUBSTRING(ec.file_name FROM -14 FOR 10), '%Y_%m_%d') DESC) rn
FROM
		tr_genres tr
LEFT JOIN ds_genres ds ON
		ds.genre_id = tr.genre_id
INNER JOIN etl_control ec 
	ON
		tr.data_load_id = ec.data_load_id;
