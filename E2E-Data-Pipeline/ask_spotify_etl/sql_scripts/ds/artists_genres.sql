INSERT
	INTO
	ds_artists_genres (genre_id,
	artist_id,
	hdif,
	data_load_id)
SELECT DISTINCT 
	tr.genre_id,
	tr.artist_id,
	tr.hdif,
	tr.data_load_id
FROM
	tr_artists_genres tr
LEFT JOIN ds_artists_genres ds
		ON	tr.hdif = ds.hdif
WHERE
	ds.hdif IS NULL;
