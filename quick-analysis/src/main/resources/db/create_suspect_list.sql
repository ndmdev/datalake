CREATE TABLE SUSPECT_LIST(
MSISDN VARCHAR(16) PRIMARY KEY,
NAME VARCHAR(16)
)
AS
SELECT * from CSVREAD('/home/krish/eclipse-workspace/datalake/quick-analysis/src/main/resources/suspectList.csv','MSISDN,NAME');