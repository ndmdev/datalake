CALL CSVWRITE('/data/result/short_call.csv', 'select A_PARTY, B_PARTY, Q_DATE, Q_TIME, DURATION_SECS, AREA_LOC, PDP_LOC, CALL_TYPE from quick_analysis where DURATION_SECS<15');