package edu.qianfeng.anlastic.hive;

import edu.qianfeng.anlastic.model.dim.base.PlatformDimension;
import edu.qianfeng.anlastic.service.IDimensionConvertor;
import edu.qianfeng.anlastic.service.impl.IDimensionConvertorImpl;
import edu.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;

/**
 * Created by lyd on 2018/6/7.
 * 平台维度的udf
 */
public class PlatformDimensionUDF extends UDF {

    private IDimensionConvertor convert = null;

    public PlatformDimensionUDF() {
        this.convert = new IDimensionConvertorImpl();
    }

    /**
     * 根据category,action获取事件维度的id
     *
     * @param platformName
     * @return
     */
    public int evaluate(String platformName) {
        if (StringUtils.isEmpty(platformName)) {
            platformName = GlobalConstants.DEFAULT_VALUE;
        }
        PlatformDimension dimension = new PlatformDimension(platformName);
        try {
            int id = convert.getDimensionIdByValue(dimension);
            return id;
        } catch (IOException e) {
            throw new RuntimeException("获取平台维度Id的udf异常.");
        }
    }

    public static void main(String[] args) {
        System.out.println(new PlatformDimensionUDF().evaluate("website"));
    }
}
