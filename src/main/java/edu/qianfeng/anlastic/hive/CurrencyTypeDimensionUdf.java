package edu.qianfeng.anlastic.hive;

import edu.qianfeng.anlastic.model.dim.base.CurrencyTypeDimension;
import edu.qianfeng.anlastic.service.IDimensionConvertor;
import edu.qianfeng.anlastic.service.impl.IDimensionConvertorImpl;
import edu.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 获取货币类型维度的Id
 * Created by lyd on 2018/4/9.
 */
public class CurrencyTypeDimensionUdf extends UDF {

    public IDimensionConvertor converter = null;

    public CurrencyTypeDimensionUdf() {
        converter = new IDimensionConvertorImpl();
    }

    /**
     * @param name
     * @return
     */
    public int evaluate(String name) {
        name = name == null || StringUtils.isEmpty(name.trim()) ? GlobalConstants.DEFAULT_VALUE : name.trim();
        CurrencyTypeDimension currencyTypeDimension = new CurrencyTypeDimension(name);
        try {
            return converter.getDimensionIdByValue(currencyTypeDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new RuntimeException("获取货币类型维度的Id异常");
    }

    public static void main(String[] args) {
        System.out.println(new CurrencyTypeDimensionUdf().evaluate(null));
    }
}
