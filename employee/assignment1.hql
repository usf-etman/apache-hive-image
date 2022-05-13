Create database assign1_youssef;

create database assign1_loc_youssef
location ‘/hp_db/assign1_loc_youssef’;

create table assign1_youssef.assign1_intern_tab
    (
    id smallint,
    name string,
    age tinyint,
    city string
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE;

load data local inpath '/employee/employee.csv' into table assign1_youssef.assign1_intern_tab;

select * from assign1_youssef.assign1_intern_tab limit 10;

CREATE EXTERNAL TABLE assign1_loc_youssef.assign1_extern_tab
    (
        id smallint,
        name string,
        age tinyint,
        city string
    )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        STORED AS TEXTFILE
        LOCATION '/Youssef/employee';

!hdfs dfs -put /employee/employee.csv /Youssef/employee;

CREATE TABLE assign1_youssef.staging
    (
    id smallint,
        name string,
        age tinyint,
        city string
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        STORED AS TEXTFILE;

load data local inpath '/employee/employee.csv' into table assign1_youssef.staging;

FROM assign1_youssef.staging
INSERT INTO assign1_youssef.assign1_intern_tab SELECT id, name, age, city
INSERT INTO assign1_loc_youssef.assign1_extern_tab SELECT id, name, age, city;

CREATE TABLE songs (
    artist_id varchar(18),
    artist_lat decimal(7,5),
    artist_loc varchar(80),
    artist_lon decimal(7,5),
    artist_name string,
    duration decimal(9,5),
    num_songs tinyint,
    song_id varchar(18),
    title string,
    year smallint)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ","
)
    TBLPROPERTIES("skip.header.line.count"="1");

SELECT * FROM songs LIMIT 10;

SELECT count(*) FROM songs;

CREATE EXTERNAL TABLE assign1_loc_youssef.ext_table
    (
        id smallint,
        name string,
        age tinyint,
        city string
    )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        STORED AS TEXTFILE
        LOCATION '/Youssef/temp';

!hdfs dfs -put /employee/employee.csv /Youssef/temp;

CREATE DATABASE new_db;
ALTER TABLE assign1_youssef.assign1_intern_tab rename to new_db.assign1_intern_tab;

!hdfs dfs -ls /user/hive/warehouse/new_db.db