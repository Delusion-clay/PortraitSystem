package com.it.userportrait.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
    public static String getByinterMinute(String timeinfo){
        Long timeMillons = Long.valueOf(timeinfo);
        Date date = new Date(timeMillons);
        DateFormat dateFormatMinute = new SimpleDateFormat("mm");
        DateFormat dateFormatHour = new SimpleDateFormat("yyyyMMddHH");
        String minute = dateFormatMinute.format(date);
        String hour = dateFormatHour.format(date);
        Long minuteLong = Long.valueOf(minute);
        String replaceMinute = "";
        if(minuteLong >= 0 && minuteLong <5){//0-5
            replaceMinute = "05";
        }else if (minuteLong >= 5 && minuteLong <10){
            replaceMinute = "10";
        }else if (minuteLong >= 10 && minuteLong <15){
            replaceMinute = "15";
        }else if (minuteLong >= 15 && minuteLong <20){
            replaceMinute = "20";
        }else if (minuteLong >= 20 && minuteLong <25){
            replaceMinute = "25";
        }else if (minuteLong >= 25 && minuteLong <30){
            replaceMinute = "30";
        }else if (minuteLong >= 30 && minuteLong <35){
            replaceMinute = "35";
        }else if (minuteLong >= 35 && minuteLong <40){
            replaceMinute = "40";
        }else if (minuteLong >= 40 && minuteLong <45){
            replaceMinute = "45";
        }else if (minuteLong >= 45 && minuteLong <50){
            replaceMinute = "50";
        }else if (minuteLong >= 50 && minuteLong <55){
            replaceMinute = "55";
        }else if (minuteLong >= 55 && minuteLong <60){
            replaceMinute = "60";
        }
        String fullTime = hour+replaceMinute;
        return fullTime;
    }

    public static Long getCurrentFiveMinuteInterStart(Long visitTime){
        String timeString = getByinterMinute(visitTime+"");
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        try {
            Date date = dateFormat.parse(timeString);
            return date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Long getCurrentHourStart(Long visitTime){
        Date date = new Date(visitTime);
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH");
        try {
            Date filterTime = dateFormat.parse(dateFormat.format(date));
            return filterTime.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Long getCurrentDayStart(Long visitTime){
        Date date = new Date(visitTime);
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        try {
            Date filterTime = dateFormat.parse(dateFormat.format(date));
            return filterTime.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Long getCurrentWeekStart(Long visitTime){
        Calendar cal =Calendar.getInstance();
        if (null != visitTime) {
            cal.setTimeInMillis(visitTime);
        }
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    public static Long getCurrentMonthStart(Long visitTime){
        Calendar cal =Calendar.getInstance();

        if (null != visitTime) {
            cal.setTimeInMillis(visitTime);
        }
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();

    }

    public static int getDaysBetweenbyStartAndend(Date startTime,Date endTime) {
        Calendar startcalendar = Calendar.getInstance();
        Calendar endcalendar = Calendar.getInstance();
        startcalendar.setTime(startTime);
        endcalendar.setTime(endTime);
        int days = 0;
        while(startcalendar.before(endcalendar)){
            startcalendar.add(Calendar.DAY_OF_YEAR,1);
            days += 1;
        }
        return days;
    }

    public static String getHourByTime(Date time) {
        DateFormat dateFormat = new SimpleDateFormat("hh");
        return dateFormat.format(time);
    }

    public static String getDateString(long time,String format){
        DateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(time);
    }

    public static String tranferDate(String time,String format) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat(format);
        DateFormat dateFormatnew = new SimpleDateFormat("yyyy-MM-dd");
        String datestring = dateFormatnew.format(dateFormat.parse(time));
        return datestring;
    }

    public static int compareDate(String o1, String o2,String dateFormatpara) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat(dateFormatpara);
        int result = (dateFormat.parse(o1)).compareTo(dateFormat.parse(o2));
        return result;

    }



}
