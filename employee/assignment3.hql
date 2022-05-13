CREATE TABLE events(
artist STRING, auth STRING, firstName String, gender STRING, itemInSession SMALLINT, lastName String, length DECIMAL(9,5), level STRING, location STRING, method STRING, page STRING, registration BIGINT, sessionId SMALLINT, song STRING, status SMALLINT, ts STRING, userAgent STRING, userId SMALLINT
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ","
)
TBLPROPERTIES("skip.header.line.count"="1");

!hdfs dfs -put /employee/events.csv /user/hive/warehouse/events;

SELECT userId, sessionId, first_song, last_song FROM
(SELECT 
userId, sessionId, first_value(song, TRUE) OVER(PARTITION BY sessionId ORDER BY ts) first_song,
last_value(song, TRUE) OVER(PARTITION BY sessionId ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_song, ROW_NUMBER() OVER(PARTITION BY sessionId ORDER BY ts DESC) rn
FROM events
ORDER BY sessionId) sq
WHERE rn = 1;

SELECT userId, distinct_songs, RANK() OVER(ORDER BY distinct_songs) rnk
FROM
(SELECT userId, COUNT(DISTINCT(song)) distinct_songs
FROM events
GROUP BY userId) sq
ORDER BY rnk;

SELECT userId, distinct_songs, ROW_NUMBER() OVER(ORDER BY distinct_songs) rnk
FROM
(SELECT userId, COUNT(DISTINCT(song)) distinct_songs
FROM events
WHERE page = 'NextSong'
GROUP BY userId) sq
ORDER BY rnk;

SELECT location, artist, cnt_loc_artist, cnt_loc, cnt_total
FROM
(SELECT location, artist, 
count(song) OVER(PARTITION BY location, artist) cnt_loc_artist, 
count(song) OVER(PARTITION BY location) cnt_loc, 
count(song) OVER() cnt_total, ROW_NUMBER() OVER(PARTITION BY location, artist) rn
FROM events) sq
WHERE rn = 1;

SELECT location, artist, cnt_loc_artist, cnt_loc, cnt_total
FROM
(SELECT location, artist, 
count(song) OVER(PARTITION BY location, artist) cnt_loc_artist, 
count(song) OVER(PARTITION BY location) cnt_loc, 
count(song) OVER(PARTITION BY artist) cnt_artist, 
count(song) OVER() cnt_total, ROW_NUMBER() OVER(PARTITION BY location, artist) rn
FROM events) sq
WHERE rn = 1;

SELECT userId, LAG(song, 1) OVER(PARTITION BY userId ORDER BY ts) prev_song, LEAD(song, 1) OVER(PARTITION BY userId ORDER BY ts) next_song
FROM events;

SELECT userId, song, ts
FROM events
ORDER BY userId, song, ts;

SELECT userId, song, ts
FROM events
SORT BY userId, song, ts;
