USE assign1_youssef;
DROP TABLE IF EXISTS staging;
CREATE TABLE staging (id smallint, name string, age tinyint, city string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
load data local inpath '/employee/employee.csv' into table staging;
