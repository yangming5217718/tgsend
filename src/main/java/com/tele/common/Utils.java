package com.tele.common;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Utils {
	
	/**
	 * 获取yyMMddHHmmss的当时时间格式
	 * @return
	 */
	public static String getCurrentDateTimeForyyyyMMddHHmmss() {
		return formatCurrentDateTime("yyyyMMddHHmmss");
	}

	/**
	 * 获取yyyyMMddHHmmss的当时时间格式加指定分钟
	 *
	 * @return
	 */
	public static String getCurrentDateTimePlusMinutes(int minutes) {
		return formatCurrentDateTimePlusMinutes("yyyyMMddHHmmss",minutes);
	}

	/**
	 * 根据传进来的格式以及加的分钟数，将时间进行格式化并返回
	 *
	 * @return
	 */
	public static String formatCurrentDateTimePlusMinutes(String formatText, int minutes) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MINUTE, minutes);
		return convertDateToStr(calendar.getTime(), formatText);
	}

	/**
	 * 根据传进来的格式，将时间进行格式化并返回
	 * 
	 * @return
	 */
	public static String formatCurrentDateTime(String formatText) {
		Calendar calendar = Calendar.getInstance();
		return convertDateToStr(calendar.getTime(), formatText);
	}
	
	/**
	 * 将日期格式进行转化，返回字符串格式
	 * 
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static String convertDateToStr(Date date, String pattern) {
		if (date == null)
			return null;
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		return sdf.format(date);
	}
	
	/**
	 * 去除后面的0
	 * @param str
	 * @return
	 */
	public static String stripTrailingZeros(Object str) {
		if (str == null || str.toString().length() == 0) {
			return "0";
		}
		if(str.toString().indexOf(".")>-1) {
			str = str.toString().replaceAll("0+?$", "");//去掉后面无用的零
			str = str.toString().replaceAll("[.]$", "");//如小数点后面全是零则去掉小数点
		}
		return str.toString();
	}

}
