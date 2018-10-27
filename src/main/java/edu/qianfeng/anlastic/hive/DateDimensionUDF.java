package edu.qianfeng.anlastic.hive;

import edu.qianfeng.anlastic.model.dim.base.DateDimension;
import edu.qianfeng.anlastic.service.IDimensionConvertor;
import edu.qianfeng.anlastic.service.impl.IDimensionConvertorImpl;
import edu.qianfeng.common.DateEnum;
import edu.qianfeng.util.TimeUtil;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by lyd on 2018/6/7.
 * 时间维度id的udf
 */
public class DateDimensionUDF extends UDF {

    private IDimensionConvertor convert = null;

    public DateDimensionUDF() {
        this.convert = new IDimensionConvertorImpl();
    }

    /**
     * 根据category,action获取事件维度的id
     *
     * @param date
     * @return
     */
    public int evaluate(Text date) {
        DateDimension dimension = DateDimension.buildDate(TimeUtil.parserString2Long(date.toString()), DateEnum.DAY);
        try {
            int id = convert.getDimensionIdByValue(dimension);
            return id;
        } catch (IOException e) {
            throw new RuntimeException("获取时间维度Id的udf异常.");
        }
    }
}
