package com.nextmining.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This class provides the various useful utilities for date.
 * 
 * @author Younggue Bae
 */
public class DateUtil {

	private static ThreadLocal<Map<String, SimpleDateFormat>> formatMap = new ThreadLocal<Map<String, SimpleDateFormat>>() {
		@Override
		protected Map<String, SimpleDateFormat> initialValue() {
			return new HashMap<String, SimpleDateFormat>();
		}
	};

	/**
	 * Converts data object into string with the specified format.
	 * 
	 * @param format
	 *          the date pattern the string is in
	 * @param date
	 *          a date object
	 * @return a formatted string representation of the date
	 */
	public static final String convertDateToString(String format, Date date) {
		SimpleDateFormat sdf = null;
		String returnValue = "";

		if (date == null) {
		} else {
			sdf = new SimpleDateFormat(format);
			returnValue = sdf.format(date);
		}

		return returnValue;
	}

	/**
	 * Converts string into date object.
	 * 
	 * @param format
	 * @param date
	 * @return
	 */
	public static final Date convertStringToDate(String format, String strDate) {
		// SimpleDateFormat sdf = new SimpleDateFormat(format, locale);
		SimpleDateFormat sdf = formatMap.get().get(format);

		if (null == sdf) {
			sdf = new SimpleDateFormat(format, Locale.ENGLISH);
			formatMap.get().put(format, sdf);
		}

		try {
			Date date = sdf.parse(strDate);

			return date;
		} catch (ParseException e) {
			System.out.println(strDate);
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Converts string into date object.
	 * 
	 * @param format
	 * @param strDate
	 * @param timezone the timezone(ex. "GMT")
	 * @return
	 */
	public static final Date convertStringToDate(String format, String strDate, String timezone) {
		SimpleDateFormat sdf = formatMap.get().get(format);

		if (null == sdf) {
			sdf = new SimpleDateFormat(format, Locale.ENGLISH);
			sdf.setTimeZone(TimeZone.getTimeZone(timezone));
			formatMap.get().put(format, sdf);
		}

		try {
			Date date = sdf.parse(strDate);

			return date;
		} catch (ParseException e) {
			System.out.println(strDate);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Adds days to date.
	 * 
	 * @param date
	 * @param day
	 * @return
	 */
	public static Date addDay(Date date, int day) {
		Calendar cal = Calendar.getInstance();

		cal.setTime(date);
		cal.add(Calendar.DAY_OF_YEAR, day);
		return cal.getTime();
	}

	/**
	 * Adds months to date.
	 *  
	 * @param date
	 * @param month
	 * @return
	 */
	public static Date addMonth(Date date, int month) {
		Calendar cal = Calendar.getInstance();

		cal.setTime(date);
		cal.add(Calendar.MONTH, month);
		return cal.getTime();
	}

	/**
	 * Converts long date to string with the specified format.
	 * 
	 * @param format
	 * @param lDate
	 * @return
	 */
	public static String convertLongToString(String format, long lDate) {
		Date date = new Date(lDate);
		return convertDateToString(format, date);
	}

	/**
	 * Gets the week of year for a given date.
	 * 
	 * @param date
	 * @return String	the week of year(ex. 201201)
	 */
	public static String getWeekOfYear(Date date) {
		if (date == null)
			return "";

		Calendar cal = new GregorianCalendar();
		cal.setTime(date);

		int year = cal.get(Calendar.YEAR);
		int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
		int month = cal.get(Calendar.MONTH);
		int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
		int maxDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		
		if (month == Calendar.DECEMBER && dayOfMonth == maxDay && weekOfYear == 1) {
			year = year + 1;
		}
		
		String result = String.valueOf(year) + String.valueOf(weekOfYear);

		if (result.length() < 6) {
			result = result.substring(0, 4) + "0" + result.substring(4);
		}

		return result;
	}

	/**
	 * Gets the start date of week for a given date.
	 * 
	 * @param date
	 * @return
	 */
	public static Date getStartDateOfWeek(Date date) {
		if (date == null)
			return null;

		return getStartDateOfWeek(getWeekOfYear(date));
	}

	/**
	 * Gets the end date of week for a given date.
	 * 
	 * @param date
	 * @return
	 */
	public static Date getEndDateOfWeek(Date date) {
		if (date == null)
			return null;

		return getEndDateOfWeek(getWeekOfYear(date));
	}

	/**
	 * Gets the start date of a given week of year.
	 * 
	 * @param weekofyear
	 * @return
	 */
	public static Date getStartDateOfWeek(String weekofyear) {
		if (weekofyear == null || weekofyear.length() != 6)
			return null;

		Calendar cal = new GregorianCalendar();

		int year = Integer.valueOf(weekofyear.substring(0, 4));
		int week = Integer.valueOf(weekofyear.substring(4, 6));
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.WEEK_OF_YEAR, week);
		cal.set(Calendar.DAY_OF_WEEK, 1);

		// DecimalFormat df = new DecimalFormat("00");
		// String month = df.format(cal.get(Calendar.MONTH) + 1);
		// String date = df.format(cal.get(Calendar.DATE));
		// return year + month + date;
		return cal.getTime();
	}

	/**
	 * Gets the end date of a given week of year.
	 * 
	 * @param weekofyear
	 * @return
	 */
	public static Date getEndDateOfWeek(String weekofyear) {
		if (weekofyear == null || weekofyear.length() != 6)
			return null;

		Calendar cal = new GregorianCalendar();

		int year = Integer.valueOf(weekofyear.substring(0, 4));
		int week = Integer.valueOf(weekofyear.substring(4, 6));
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.WEEK_OF_YEAR, week);
		cal.set(Calendar.DAY_OF_WEEK, 7);

		// DecimalFormat df = new DecimalFormat("00");
		// String month = df.format(cal.get(Calendar.MONTH) + 1);
		// String date = df.format(cal.get(Calendar.DATE));
		// return year + month + date;

		return cal.getTime();
	}

	/**
	 * Gets the month list with the specified format between start and end.
	 * 
	 * @param format	the date format(ex. "yyyyMM")
	 * @param start	the start date
	 * @param end	the end date
	 * @return
	 */
	public static List<String> getMonthList(String format, Date start, Date end) {
		List<String> months = new ArrayList<String>();

		months.add(DateUtil.convertDateToString(format, start));

		Date month = start;
		while (month.before(end)) {
			month = DateUtil.addMonth(month, 1);
			months.add(DateUtil.convertDateToString(format, month));
		}

		return months;
	}
	
	/**
	 * Gets the week list with the specified format between start and end.
	 * 
	 * @param format the format(ex. "yyyyMMdd")
	 * @param start
	 * @param end
	 * @return
	 */
	public static Map<String, String> getWeekList(String format, Date start, Date end) {
		return getWeekList(format, null, start, end);
	}
	
	/**
	 * Gets the week list with the specified format between start and end.
	 * 
	 * @param format the format(ex. "yyyyMMdd")
	 * @param delimiter
	 * @param start
	 * @param end
	 * @return
	 */
	public static Map<String, String> getWeekList(String format, String delimiter, Date start, Date end) {
		if (delimiter == null)
			delimiter = "";
		
		Set<String> set = new HashSet<String>();
		
		set.add(DateUtil.getWeekOfYear(start));

		Date date = start;
		while (date.before(end)) {
			date = DateUtil.addDay(date, 1);
			set.add(DateUtil.getWeekOfYear(date));
		}
		
		List<String> weeks = new ArrayList<String>(set);
		Collections.sort(weeks);
		
		Map<String, String> result = new LinkedHashMap<String, String>();
		for (String weekofyear : weeks) {
			String startofweek = DateUtil.convertDateToString(format, DateUtil.getStartDateOfWeek(weekofyear));
			String endofweek = DateUtil.convertDateToString(format, DateUtil.getEndDateOfWeek(weekofyear));
			result.put(weekofyear, startofweek + delimiter + endofweek);
		}
			
		return result;
	}
	
	/**
	 * Gets the week with the specified format for a given date.
	 * 
	 * @param format format the format(ex. "yyyyMMdd")
	 * @param date
	 * @return
	 */
	public static String getWeek(String format, Date date) {
		return getWeek(format, null, date);
	}
	
	/**
	 * Gets the week with the specified format for a given date.
	 * 
	 * @param format format the format(ex. "yyyyMMdd")
	 * @param delimiter
	 * @param date
	 * @return
	 */
	public static String getWeek(String format, String delimiter, Date date) {
		if (date == null)
			return null;
		
		if (delimiter == null)
			delimiter = "";
		
		String weekofyear = DateUtil.getWeekOfYear(date);
		String startofweek = DateUtil.convertDateToString(format, DateUtil.getStartDateOfWeek(weekofyear));
		String endofweek = DateUtil.convertDateToString(format, DateUtil.getEndDateOfWeek(weekofyear));
		
		return startofweek + delimiter + endofweek;
	}
	
	/**
	 * Gets the end date of month for a given date.
	 * 
	 * @param date
	 * @return
	 */
	public static Date getEndDateOfMonth(Date date) {
		if (date == null)
			return null;

		Calendar cal = new GregorianCalendar();
		cal.setTime(date);

		int maxDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		cal.set(Calendar.DAY_OF_MONTH, maxDay);

		return cal.getTime();
	}
	
	/**
	 * Returns true if the date is start of the week.
	 * 
	 * @param date
	 * @return
	 */
	public static boolean isStartOfWeek(Date date) {
		if (date == null)
			return false;

		Calendar cal = new GregorianCalendar();
		cal.setTime(date);
		
		if (cal.get(Calendar.DAY_OF_WEEK) == 1) {
			return true;
		}

		return false;
	}
	
	/**
	 * Returns true if the date is start of the week.
	 * 
	 * @param date
	 * @param span
	 * @return
	 */
	public static boolean isStartOfWeek(Date date, int span) {
		if (date == null)
			return false;

		Calendar cal = new GregorianCalendar();
		cal.setTime(date);
		
		if (cal.get(Calendar.DAY_OF_WEEK) <= span) {
			return true;
		}

		return false;
	}
	
	/**
	 * Returns true if the date is end of the week.
	 * 
	 * @param date
	 * @return
	 */
	public static boolean isEndOfWeek(Date date) {
		if (date == null)
			return false;

		Calendar cal = new GregorianCalendar();
		cal.setTime(date);
		
		if (cal.get(Calendar.DAY_OF_WEEK) == 7) {
			return true;
		}

		return false;
	}
	
	/**
	 * Returns true if the date is start of the month.
	 * 
	 * @param date
	 * @return
	 */
	public static boolean isStartOfMonth(Date date) {
		if (date == null)
			return false;

		Calendar cal = new GregorianCalendar();
		cal.setTime(date);

		if (cal.get(Calendar.DAY_OF_MONTH) == 1) {
			return true;
		}
		return false;
	}
	
	/**
	 * Returns true if the date is start of the month.
	 * 
	 * @param date
	 * @param span
	 * @return
	 */
	public static boolean isStartOfMonth(Date date, int span) {
		if (date == null)
			return false;

		Calendar cal = new GregorianCalendar();
		cal.setTime(date);

		if (cal.get(Calendar.DAY_OF_MONTH) <= span) {
			return true;
		}
		return false;
	}
	
	/**
	 * Returns true if the date is end of the month.
	 * 
	 * @param date
	 * @return
	 */
	public static boolean isEndOfMonth(Date date) {
		if (date == null)
			return false;

		Calendar cal = new GregorianCalendar();
		cal.setTime(date);

		int maxDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		if (cal.get(Calendar.DAY_OF_MONTH) == maxDay) {
			return true;
		}
		return false;
	}
	
	/**
	 * Returns the day of week for the given date.
	 * (1:SUN, 2:MON, 3:TUE, 4:WED, 5:THU, 6:FRI, 7:SAT)
	 * 
	 * @param date
	 * @return
	 */
	public static int getDayOfWeek(Date date) {
		Calendar cal = Calendar.getInstance();
		//Calendar cal = new GregorianCalendar();
		cal.setTime(date);
		
		int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
		
		return dayOfWeek;
	}
	
	public static long getTimeSpan(Date start, Date end) {
		long diffInSeconds = (end.getTime() - start.getTime()) / 1000;

		/*
    long diff[] = new long[] { 0, 0, 0, 0 };
    // sec
    diff[3] = (diffInSeconds >= 60 ? diffInSeconds % 60 : diffInSeconds);
    // min
    diff[2] = (diffInSeconds = (diffInSeconds / 60)) >= 60 ? diffInSeconds % 60 : diffInSeconds;
    // hours
    diff[1] = (diffInSeconds = (diffInSeconds / 60)) >= 24 ? diffInSeconds % 24 : diffInSeconds;
    // days
    diff[0] = (diffInSeconds = (diffInSeconds / 24));

    System.out.println(String.format(
        "%d day%s, %d hour%s, %d minute%s, %d second%s ago",
        diff[0],
        diff[0] > 1 ? "s" : "",
        diff[1],
        diff[1] > 1 ? "s" : "",
        diff[2],
        diff[2] > 1 ? "s" : "",
        diff[3],
        diff[3] > 1 ? "s" : ""));
      */
		
		return diffInSeconds;
	}

	public static void main(String[] args) {
		Date date = DateUtil.convertStringToDate("yyyyMMdd", "20121117");
		System.out.println("date == " + date);
		String weekOfYear = DateUtil.getWeekOfYear(date);
		System.out.println("week of year == " + weekOfYear);

		System.out.println("start date of week for " + weekOfYear + " == " + DateUtil.getStartDateOfWeek(weekOfYear));
		System.out.println("start date of week == " + DateUtil.getStartDateOfWeek(date));
		System.out.println("end date of week == " + DateUtil.getEndDateOfWeek(date));

		String strDate = "2012-07-24T00:14:33.000Z";
		System.out.println("converted date == " + DateUtil.convertStringToDate("yyyy-MM-dd'T'HH:mm:ss'.000Z'", strDate));

		System.out.println("add month == " + DateUtil.addMonth(date, 2));
		System.out.println("months == " + DateUtil.getMonthList("yyyy-MM", date, DateUtil.addMonth(date, 12)));
		
		System.out.println("weeks == " + DateUtil.getWeekList("yyyyMMdd", "~", date, DateUtil.addMonth(date, 12)));
		
		System.out.println("end date of month == " + DateUtil.getEndDateOfMonth(date));
		
		System.out.println("original date == " + date);
		
		date = DateUtil.convertStringToDate("yyyyMMddHHmmss", "20130810183010");
		System.out.println("date == " + date);
		System.out.println("day of week == " + DateUtil.getDayOfWeek(date));
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int hours = calendar.get(Calendar.HOUR_OF_DAY);
		System.out.println("hours == " + hours);
		
	}
}
