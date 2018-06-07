package edu.qianfeng.common;

/**
 * Created by lyd on 2018/5/31.
 * 时间枚举
 */
public enum  DateEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public String name;

    DateEnum(String name) {
        this.name = name;
    }

    //TODO

}
