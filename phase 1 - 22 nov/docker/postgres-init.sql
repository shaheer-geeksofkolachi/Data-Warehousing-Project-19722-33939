-- runs once when the postgres volume is created
CREATE DATABASE airflow;
CREATE DATABASE pharma_sdi;
ALTER DATABASE pharma_sdi SET timezone TO 'UTC';
