CALL CSVWRITE('/data/result/imei_used.csv', 'select ROWNUM, A_PARTY, IMEI, MIN(Q_DATE) START_DATE, MAX(Q_DATE) END_DATE from quick_analysis group by IMEI');