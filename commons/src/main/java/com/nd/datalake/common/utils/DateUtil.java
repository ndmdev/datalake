package com.nd.datalake.common.utils;

import java.util.Calendar;

/**
 * 
 * @author krishnaprasad
 *
 */
public abstract class DateUtil {

	/**
	 *
	 * @param dayShift
	 *            , how much to add to the current date eg: 1 for next day start
	 * @return Day start in Milliseconds
	 */
	public static long getDayStart(final int dayShift) {
		final Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, dayShift * 24);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTimeInMillis();
	}

}