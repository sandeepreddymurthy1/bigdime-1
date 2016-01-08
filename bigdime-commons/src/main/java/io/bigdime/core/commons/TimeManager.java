/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Helper class to get the local time and UTC
 *
 * @author jbrinand/mnamburi
 *
 */
public final class TimeManager {
	public static final String FORMAT_YYYYMMDD = "yyyyMMdd";

	private TimeManager() { }
	private static TimeManager instance;
	public static TimeManager getInstance() {
		if (instance == null) {
			instance = new TimeManager();
		}
		return instance;
	}
	/**
	 *  find local time and return it.
	 * @return
	 */
	public DateTime getLocalDateTime(){
		DateTime now = new DateTime(DateTimeZone.getDefault());
		return now;
	}
	/**
	 * get current UTC time.
	 * @return
	 */
	public DateTime getCurrentUTCDate() {
		DateTime dt = new DateTime(DateTimeZone.UTC);
		return dt;
	}
	/**
	 * apply the format and returns the string value
	 */
	public String format(String pattern, DateTime dateTime) {
		DateTimeFormatter fmt = DateTimeFormat.forPattern(pattern);
		String dateFormat = fmt.print(dateTime).trim();				
		return dateFormat;
	}
}
