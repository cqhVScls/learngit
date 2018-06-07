package edu.qianfeng.anlastic.hive;

import edu.qianfeng.anlastic.model.dim.base.EventDimension;
import edu.qianfeng.anlastic.service.IDimensionConvertor;
import edu.qianfeng.anlastic.service.impl.IDimensionConvertorImpl;
import edu.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;

/**
 * Created by lyd on 2018/6/7.
 * eventDimensionId的udf
 */
public class EventDimensionUDF extends UDF{

    private IDimensionConvertor convert = null;

    public EventDimensionUDF(){
        this.convert = new IDimensionConvertorImpl();
    }

    /**
     * 根据category,action获取事件维度的id
     * @param category
     * @param action
     * @return
     */
    public int evaluate(String category,String action){
        if(StringUtils.isEmpty(category)){
            category = action = GlobalConstants.DEFAULT_VALUE;
        }
        if(StringUtils.isEmpty(action)){
            action = GlobalConstants.DEFAULT_VALUE;
        }
        EventDimension dimension = new EventDimension(category,action);
        try {
            int id = convert.getDimensionIdByValue(dimension);
            return id;
        } catch (IOException e) {
            throw new RuntimeException("获取事件维度Id的udf异常.");
        }
    }
}
