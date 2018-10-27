package edu.qianfeng.util;

import edu.qianfeng.common.DateEnum;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by lyd on 2018/5/30.
 * 时间工具类
 */
public class TimeUtil {
    public static final String DATE_FORMAT = "yyyy-MM-dd";  //默认的日期格式

    public static String getYesterday() {
        return getYesterday(DATE_FORMAT);
    }

    /**
     * 获取指定格式的昨天的日期
     *
     * @param pattern
     * @return
     */
    public static String getYesterday(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(cal.getTime());
    }

    /**
     * 判断输入的日期是否可用
     *
     * @param input yyyy-MM-dd
     * @return 可用返回true，反之返回false
     */
    public static boolean isValidRunningDate(String input) {
        Matcher matcher = null;
        boolean result = false;
        String regexp = "^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}";
        if (input != null && !input.isEmpty()) {
            Pattern pattern = Pattern.compile(regexp);
            matcher = pattern.matcher(input);
        }
        if (matcher != null) {
            result = matcher.matches();
        }
        return result;
    }

    /**
     * 将字符串的日期转换为时间搓
     *
     * @param input 输入的日期
     * @return
     */
    public static long parserString2Long(String input) {
        return parserString2Long(input, DATE_FORMAT);
    }

    /**
     * 按照指定的pattern将日期转换成时间戳
     *
     * @param input
     * @param pattern
     * @return
     */
    public static long parserString2Long(String input, String pattern) {
        Date date = null;
        try {
            date = new SimpleDateFormat(pattern).parse(input);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    /**
     * 将时间戳转换成字符串的日期
     *
     * @param input
     * @return
     */
    public static String parserLong2String(long input) {
        return parserLong2String(input, DATE_FORMAT);
    }

    /**
     * 将时间戳转换成指定格式的日期
     *
     * @param input
     * @param pattern
     * @return
     */
    public static String parserLong2String(long input, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(input);
        return new SimpleDateFormat(pattern).format(calendar.getTime());
    }

    /**
     * 根据时间戳和type返回对应的值
     *
     * @param time
     * @param type
     * @return
     */
    public static int getDateInfo(long time, DateEnum type) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (type.equals(DateEnum.YEAR)) {
            return calendar.get(Calendar.YEAR);
        }
        if (type.equals(DateEnum.SEASON)) {
            int month = calendar.get(Calendar.MONTH) + 1; //从0开始
            if (month % 3 == 0) {
                return month / 3;
            }
            return month / 3 + 1;
        }
        if (type.equals(DateEnum.MONTH)) {
            return calendar.get(Calendar.MONTH) + 1;
        }
        if (type.equals(DateEnum.WEEK)) {
            return calendar.get(Calendar.WEEK_OF_YEAR);
        }
        if (type.equals(DateEnum.DAY)) {
            return calendar.get(Calendar.DAY_OF_MONTH);
        }
        if (type.equals(DateEnum.HOUR)) {
            return calendar.get(Calendar.HOUR_OF_DAY);
        }
        throw new RuntimeException("不能支持该type的转换,type:" + type.name);
    }

    /**
     * 获取当前周的第一天的时间戳
     *
     * @param time
     * @return
     */
    public static long getFirstDayOfWeek(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.DAY_OF_WEEK, 1); //设置周的第一天
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis(); //返回毫秒
    }

    public static void main(String[] args) {
//        System.out.println(getYesterday("yyyy/MM/dd"));
//        System.out.println(isValidRunningDate("20a8-5-30"));
//        System.out.println(parserString2Long("2018-05-30","yyyy-MM-dd"));
//        System.out.println(parserLong2String(1527609600000l,"yyyy-MM-dd"));
//        System.out.println(getDateInfo(1527754913000l,DateEnum.WEEK));
        System.out.println(getFirstDayOfWeek(1527754913000l));
    }
}
