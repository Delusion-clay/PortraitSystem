package com.it.userportrait.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class YearsUtils {
    public  static String getYears(int age){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.YEAR,-Integer.valueOf(age));
        Date newdate = calendar.getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyy");
        String newdatestring = dateFormat.format(newdate);
        Integer newdateinteger = Integer.valueOf(newdatestring);
        String yearbasetype = "未知";
        if(newdateinteger >= 1940 && newdateinteger < 1950){
            yearbasetype = "40后";
        }else if (newdateinteger >= 1950 && newdateinteger < 1960){
            yearbasetype = "50后";
        }else if (newdateinteger >= 1960 && newdateinteger < 1970){
            yearbasetype = "60后";
        }else if (newdateinteger >= 1970 && newdateinteger < 1980){
            yearbasetype = "70后";
        }else if (newdateinteger >= 1980 && newdateinteger < 1990){
            yearbasetype = "80后";
        }else if (newdateinteger >= 1990 && newdateinteger < 2000){
            yearbasetype = "90后";
        }else if (newdateinteger >= 2000 && newdateinteger < 2010){
            yearbasetype = "00后";
        }else if (newdateinteger >= 2010 ){
            yearbasetype = "10后";
        }
        return yearbasetype;
    }
}
