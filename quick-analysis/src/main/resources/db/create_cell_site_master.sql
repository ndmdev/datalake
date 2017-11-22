CREATE TABLE CELL_SITE_MASTER(
CELL_ID VARCHAR(32) PRIMARY KEY,
LOCATION VARCHAR(64)
)
AS
SELECT * from CSVREAD('/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/cell-site-master.csv','CELL_ID,LOCATION');