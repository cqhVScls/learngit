package edu.qianfeng.anlastic.model.dim.base;

import org.apache.hadoop.io.WritableComparable;

/**
 * Created by lyd on 2018/5/31.
 * 维度信息的顶级父类（相当于自定义数据类型的实体bean）
 * 它下面有浏览器维度、时间维度、平台维度等。
 */
public abstract class BaseDimension implements WritableComparable<BaseDimension> {
    //do nothing
}
