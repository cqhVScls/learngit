package edu.qianfeng.anlastic.mr;

import edu.qianfeng.anlastic.model.dim.base.BaseDimension;
import edu.qianfeng.anlastic.model.value.BaseStatsValueWritable;
import edu.qianfeng.anlastic.service.IDimensionConvertor;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by lyd on 2018/6/1.
 * 操作结果表的输出接口
 */
public interface IOuputCollector {
    /**
     * 操作结果表的sql的赋值
     * @param conf
     * @param key
     * @param value
     * @param ps
     * @param convertor
     * @throws IOException
     * @throws SQLException
     */
    void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value,
                 PreparedStatement ps, IDimensionConvertor convertor) throws IOException,SQLException;
}
