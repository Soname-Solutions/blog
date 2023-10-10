CREATE OR REPLACE
VIEW incr_v_artists AS
	SELECT
		tr.artist_id,
		tr.artist_src_id,
		tr.artist_nm,
		tr.artist_popularity,
		tr.artist_followers,
		tr.hdif,
		tr.data_load_id,
		ds.artist_id AS ds_artist_id,
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
		tr.data_load_id = ec.data_load_id;
