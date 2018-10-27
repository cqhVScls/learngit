package edu.qianfeng.anlastic.mr.pv;

import edu.qianfeng.anlastic.model.dim.StatsUserDimension;
import edu.qianfeng.anlastic.model.dim.base.BaseDimension;
import edu.qianfeng.anlastic.model.value.BaseStatsValueWritable;
import edu.qianfeng.anlastic.model.value.MapWritableValue;
import edu.qianfeng.anlastic.mr.IOuputCollector;
import edu.qianfeng.anlastic.service.IDimensionConvertor;
import edu.qianfeng.common.GlobalConstants;
import edu.qianfeng.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by lyd on 2018/6/4.
 * pageview的sql进行赋值
 */
public class PageViewCollector implements IOuputCollector {
    @Override
    public void collect(Configuration conf, BaseDimension key,
                        BaseStatsValueWritable value, PreparedStatement ps,
                        IDimensionConvertor convertor) throws IOException, SQLException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        IntWritable pageviews = (IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-1));
        int i = 0;
        if (value.getKpi().equals(KpiType.PAGE_VIEW)) {
            //设置值
            ps.setInt(++i, convertor.getDimensionIdByValue(((StatsUserDimension) key).getStatsCommon().getDateDimension()));
            ps.setInt(++i, convertor.getDimensionIdByValue(((StatsUserDimension) key).getStatsCommon().getPlatformDimension()));
            ps.setInt(++i, convertor.getDimensionIdByValue(((StatsUserDimension) key).getBrowser()));
            ps.setInt(++i, pageviews.get());
            ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_FORMAT));
            ps.setInt(++i, pageviews.get());
            //加载到bathc中
            ps.addBatch();
        }
    }
}
