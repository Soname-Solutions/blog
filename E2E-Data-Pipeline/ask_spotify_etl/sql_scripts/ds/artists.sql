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
SELECT
	tr.artist_id,
	tr.artist_src_id,
	tr.artist_nm,
	tr.artist_popularity,
	tr.artist_followers,
	tr.hdif,
	tr.data_load_id
FROM
	tr_artists tr
LEFT JOIN ds_artists ds
	ON tr.artist_id = ds.artist_id
WHERE
	ds.artist_id IS NULL;


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
		tr.data_load_id
	FROM
		tr_artists tr
	LEFT JOIN ds_artists ds
		ON
		tr.artist_id = ds.artist_id
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
	AND ds.hdif != tr_data.hdif;

SET FOREIGN_KEY_CHECKS=1;
