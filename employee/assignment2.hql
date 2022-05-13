CREATE DATABASE assign2;
use assign2;

CREATE TABLE songs_part (
    artist_id VARCHAR(18),
    artist_latitude DECIMAL(7,5),
    artist_loc VARCHAR(80),
    artist_longitude DECIMAL(7,5),
    duration DECIMAL(9,5),
    num_songs TINYINT,
    song_id VARCHAR(18),
    title STRING)
PARTITIONED BY(year SMALLINT, artist_name STRING)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ","
)
    TBLPROPERTIES("skip.header.line.count"="1");

!hdfs dfs -put /employee/songs.csv /user/hive/warehouse/assign2.db/songs_part;

ALTER TABLE songs_part ADD PARTITION(artist='amr diab', year='1999')
LOCATION '/Youssef/songs/amr diab/1999';

LOAD DATA LOCAL INPATH '/employee/songs.csv' INTO TABLE songs_part
PARTITION (artist='amr diab', year='1999');

!hdfs dfs -ls /Youssef/songs/amr*/1999;

FROM assign1_youssef.songs src
INSERT OVERWRITE TABLE songs_part PARTITION (year, artist_name)
SELECT src.artist_id, src.artist_lat, src.artist_loc, src.artist_lon, src.duration, src.num_songs, src.song_id, src.title, src.year, src.artist_name;

truncate table songs_part;

FROM assign1_youssef.songs src
INSERT OVERWRITE TABLE songs_part PARTITION (year, artist_name)
SELECT src.artist_id, src.artist_lat, src.artist_loc, src.artist_lon, src.duration, src.num_songs, src.song_id, src.title, src.year, src.artist_name;

truncate table songs_part;

FROM assign1_youssef.songs
INSERT INTO songs_part partition(year='1987', artist_name)
SELECT artist_id, artist_lat, artist_loc, artist_lon, duration, num_songs, song_id, title, artist_name
WHERE year='1987';

Use assign1_youssef;
CREATE TABLE staging_avro LIKE songs;
ALTER TABLE staging_avro
SET SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe';

CREATE TABLE staging_parquet LIKE songs;
ALTER TABLE staging_parquet
SET SERDE 'parquet.hive.serde.ParquetHiveSerDe';