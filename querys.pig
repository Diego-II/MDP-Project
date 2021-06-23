big5 = LOAD 'hdfs://cm:9000/uhadoop2021/grupo_patos/data-final.tsv' USING PigStorage('\t') AS (EXT1,EXT2,EXT3,EXT4,EXT5,EXT6,EXT7,EXT8,EXT9,EXT10,EST1,EST2,EST3,EST4,EST5,EST6,EST7,EST8,EST9,EST10,AGR1,AGR2,AGR3,AGR4,AGR5,AGR6,AGR7,AGR8,AGR9,AGR10,CSN1,CSN2,CSN3,CSN4,CSN5,CSN6,CSN7,CSN8,CSN9,CSN10,OPN1,OPN2,OPN3,OPN4,OPN5,OPN6,OPN7,OPN8,OPN9,OPN10,EXT1_E,EXT2_E,EXT3_E,EXT4_E,EXT5_E,EXT6_E,EXT7_E,EXT8_E,EXT9_E,EXT10_E,EST1_E,EST2_E,EST3_E,EST4_E,EST5_E,EST6_E,EST7_E,EST8_E,EST9_E,EST10_E,AGR1_E,AGR2_E,AGR3_E,AGR4_E,AGR5_E,AGR6_E,AGR7_E,AGR8_E,AGR9_E,AGR10_E,CSN1_E,CSN2_E,CSN3_E,CSN4_E,CSN5_E,CSN6_E,CSN7_E,CSN8_E,CSN9_E,CSN10_E,OPN1_E,OPN2_E,OPN3_E,OPN4_E,OPN5_E,OPN6_E,OPN7_E,OPN8_E,OPN9_E,OPN10_E,dateload,screenw,screenh,introelapse,testelapse,endelapse,IPC,country,lat_appx_lots_of_err,long_appx_lots_of_err);

-- Line 1: Filter raw to make sure type equals 'THEATRICAL_MOVIE' 
movies = FILTER raw BY type == 'THEATRICAL_MOVIE';

-- Line 2: Generate new relation with full movie name (concatenating title+##+year+##+num) and actor
full_movies = FOREACH movies GENERATE CONCAT(title,'##',year,'##',num), actor;

-- Line 3 + 4: Generate the co-star pairs 
full_movies_alias = FOREACH full_movies GENERATE $0, $1;
movie_actor_pairs = JOIN full_movies BY $0, full_movies_alias by $0;

-- Line 5: filter to ensure that the first co-star is lower alphabetically than the second
movie_actor_pairs_unique = FILTER movie_actor_pairs BY full_movies::actor < full_movies_alias::actor;

-- Line 6: concatenate the co-stars into one column 
simply_actor_pairs = FOREACH movie_actor_pairs_unique GENERATE CONCAT(full_movies::actor,'##',full_movies_alias::actor) AS actor_pair;

-- Line 7: group the relation by co-stars
actor_pairs_grouped = GROUP simply_actor_pairs BY actor_pair;

-- Line 8: count each group of co-stars
actor_pair_count = FOREACH actor_pairs_grouped GENERATE COUNT($1) AS count, group AS actor_pair; 

-- Line 9: order the count in descending order
ordered_actor_pair_count = ORDER actor_pair_count BY count DESC; 

-- output the final count
-- TODO: REPLACE ahogan WITH YOUR FOLDER
STORE ordered_actor_pair_count INTO 'hdfs://cm:9000/uhadoop2021/ahogan/imdb-costars/';
