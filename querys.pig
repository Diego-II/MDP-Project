big5 = LOAD 'hdfs://cm:9000/uhadoop2021/grupo_patos/data-final.tsv' USING PigStorage('\t') AS (EXT1,EXT2,EXT3,EXT4,EXT5,EXT6,EXT7,EXT8,EXT9,EXT10,EST1,EST2,EST3,EST4,EST5,EST6,EST7,EST8,EST9,EST10,AGR1,AGR2,AGR3,AGR4,AGR5,AGR6,AGR7,AGR8,AGR9,AGR10,CSN1,CSN2,CSN3,CSN4,CSN5,CSN6,CSN7,CSN8,CSN9,CSN10,OPN1,OPN2,OPN3,OPN4,OPN5,OPN6,OPN7,OPN8,OPN9,OPN10,EXT1_E,EXT2_E,EXT3_E,EXT4_E,EXT5_E,EXT6_E,EXT7_E,EXT8_E,EXT9_E,EXT10_E,EST1_E,EST2_E,EST3_E,EST4_E,EST5_E,EST6_E,EST7_E,EST8_E,EST9_E,EST10_E,AGR1_E,AGR2_E,AGR3_E,AGR4_E,AGR5_E,AGR6_E,AGR7_E,AGR8_E,AGR9_E,AGR10_E,CSN1_E,CSN2_E,CSN3_E,CSN4_E,CSN5_E,CSN6_E,CSN7_E,CSN8_E,CSN9_E,CSN10_E,OPN1_E,OPN2_E,OPN3_E,OPN4_E,OPN5_E,OPN6_E,OPN7_E,OPN8_E,OPN9_E,OPN10_E,dateload,screenw,screenh,introelapse,testelapse,endelapse,IPC,country,lat_appx_lots_of_err,long_appx_lots_of_err);

agg_big5 = FOREACH big5 GENERATE (EXT1-EXT2+EXT3-EXT4+EXT5-EXT6+EXT7-EXT8+EXT9-EXT10) AS EXT, (EST1-EST2+EST3-EST4+EST5+EST6+EST7+EST8+EST9+EST10) AS EST, (AGR2-AGR1-AGR3+AGR4-AGR5+AGR6-AGR7+AGR8+AGR9+AGR10) AS AGR,(CSN1-CSN2+CSN3-CSN4+CSN5-CSN6+CSN7-CSN8+CSN9+CSN10) AS CSN,(OPN1-OPN2+OPN3-OPN4+OPN5-OPN6+OPN7+OPN8+OPN9+OPN10) AS OPN, country AS country, lat_appx_lots_of_err as latitud,long_appx_lots_of_err AS longitud;

STORE agg_big5 INTO 'hdfs://cm:9000/uhadoop2021/grupo_patos/agg' USING PigStorage('\t','-schema');

fs -getmerge -nl hdfs://cm:9000/uhadoop2021/grupo_patos/agg /data/2021/uhadoop/grupo_patos/agg_big5.tsv;

by_country = GROUP agg_big5 BY country;

agg_country_big5 = FOREACH by_country GENERATE FLATTEN(AVG(agg_big5.EXT)) AS EXT,FLATTEN(AVG(agg_big5.EST)) AS EST,FLATTEN(AVG(agg_big5.AGR)) AS AGR,FLATTEN(AVG(agg_big5.CSN)) AS CSN,FLATTEN(AVG(agg_big5.OPN)) AS OPN,FLATTEN(COUNT($1)) AS count_res, FLATTEN(group) as country;

STORE agg_country_big5 INTO 'hdfs://cm:9000/uhadoop2021/grupo_patos/agg_country' USING PigStorage('\t','-schema');

fs -getmerge -nl hdfs://cm:9000/uhadoop2021/grupo_patos/agg_country /data/2021/uhadoop/grupo_patos/agg_country_big5.tsv;