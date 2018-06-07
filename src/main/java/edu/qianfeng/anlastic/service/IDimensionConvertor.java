package edu.qianfeng.anlastic.service;

import edu.qianfeng.anlastic.model.dim.base.BaseDimension;

import java.io.IOException;

/**
 * Created by lyd on 2018/6/1.
 * 操作维度表的接口。
 * 根据不同的维度对象来取出对应的维度表的维度ID，如果该维度对象没有维度ID，则插入，并返回维度，
 */
public interface IDimensionConvertor {
    /**
     * 根据维度对象取出对应的维度表的ID值，如没有则插入再查询，如有直接获取ID
     * @param baseDimension
     * @return
     * @throws IOException
     */
    int getDimensionIdByValue(BaseDimension baseDimension) throws IOException;
}
