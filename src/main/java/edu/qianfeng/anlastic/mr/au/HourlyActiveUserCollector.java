package edu.qianfeng.anlastic.mr.au;

import edu.qianfeng.anlastic.model.dim.StatsUserDimension;
import edu.qianfeng.anlastic.model.dim.base.BaseDimension;
import edu.qianfeng.anlastic.model.value.BaseStatsValueWritable;
import edu.qianfeng.anlastic.model.value.MapWritableValue;
import edu.qianfeng.anlastic.mr.IOuputCollector;
import edu.qianfeng.anlastic.service.IDimensionConvertor;
import edu.qianfeng.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by lyd on 2018/6/6.
 * 按小时统计的赋值
 */
public class HourlyActiveUserCollector implements IOuputCollector {
    @Override
    public void collect(Configuration conf, BaseDimension key,
                        BaseStatsValueWritable value, PreparedStatement ps,
                        IDimensionConvertor convertor) throws IOException, SQLException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        MapWritable map = mapWritableValue.getValue();

        int i = 0;
        //设置值
        ps.setInt(++i, convertor.getDimensionIdByValue(((StatsUserDimension) key).getStatsCommon().getDateDimension()));
        ps.setInt(++i, convertor.getDimensionIdByValue(((StatsUserDimension) key).getStatsCommon().getPlatformDimension()));
        ps.setInt(++i, convertor.getDimensionIdByValue(((StatsUserDimension) key).getStatsCommon().getKpiDimension()));
        for (i++; i < 28; i++) {
            int hourActiveUsers = ((IntWritable) map.get(new IntWritable(i - 4))).get();
            ps.setInt(i, hourActiveUsers);
            //设置29-52
            ps.setInt(i + 25, hourActiveUsers);
        }
        ps.setString(i, conf.get(GlobalConstants.RUNNING_DATE_FORMAT));
        //加载到bathc中
        ps.addBatch();
    }
}
