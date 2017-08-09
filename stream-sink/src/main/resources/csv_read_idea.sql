CREATE TABLE QUICK_ANALYSIS (
A_PARTY BIGINT,
LRN INT,
B_PARTY VARCHAR(12),
Q_DATE VARCHAR(10),
Q_TIME VARCHAR(15),
DURATION_SECS INT,
AREA_CODE VARCHAR(16),
PDP_ADDRS VARCHAR(16),
CALL_TYPE VARCHAR(16),
IMEI BIGINT,
IMSI BIGINT,
CON_TYPE VARCHAR(10),
SMS_CENTRE_NUM BIGINT,
PLMN_ID INT,
CALL_ACCESS_TYPE VARCHAR(3),
RAC INT
)
AS 
 SELECT
A_PARTY ,
CASEWHEN (LRN='N/A',null,LRN)  AS LRN,
B_PARTY ,
Q_DATE ,
Q_TIME ,
DURATION_SECS ,
AREA_CODE ,
PDP_ADDRS ,
CALL_TYPE ,
IMEI ,
IMSI ,
CON_TYPE ,
CASEWHEN (SMS_CENTRE_NUM='N/A',null,SMS_CENTRE_NUM) AS SMS_CENTRE_NUM ,
CASEWHEN (PLMN_ID='N/A',null,PLMN_ID) AS PLMN_ID ,
CALL_ACCESS_TYPE ,
CASEWHEN (RAC='N/A',null,RAC) AS RAC
FROM CSVREAD('/home/krish/Downloads/nd/cdrsamples/idea.csv', 'A_PARTY ,LRN ,B_PARTY ,Q_DATE ,Q_TIME ,DURATION_SECS ,AREA_CODE ,PDP_ADDRS ,CALL_TYPE ,IMEI ,IMSI ,CON_TYPE ,SMS_CENTRE_NUM ,PLMN_ID ,CALL_ACCESS_TYPE ,RAC');
