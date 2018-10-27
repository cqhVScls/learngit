package edu.qianfeng.anlastic.mr.local;

import edu.qianfeng.anlastic.model.dim.StatsLocationDimension;
import edu.qianfeng.anlastic.model.dim.base.BaseDimension;
import edu.qianfeng.anlastic.model.value.BaseStatsValueWritable;
import edu.qianfeng.anlastic.model.value.LocationOutputWritableValue;
import edu.qianfeng.anlastic.mr.IOuputCollector;
import edu.qianfeng.anlastic.service.IDimensionConvertor;
import edu.qianfeng.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by lyd on 2018/6/7.
 */
public class LocationCollector implements IOuputCollector {
    @Override
    public void collect(Configuration conf, BaseDimension key,
                        BaseStatsValueWritable value, PreparedStatement ps,
                        IDimensionConvertor convertor) throws IOException, SQLException {
        StatsLocationDimension statsLocationDimension = (StatsLocationDimension) key;
        LocationOutputWritableValue v = (LocationOutputWritableValue) value;
        int i = 0;
        //设置值
        ps.setInt(++i, convertor.getDimensionIdByValue(statsLocationDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(++i, convertor.getDimensionIdByValue(statsLocationDimension.getStatsCommonDimension().getPlatformDimension()));
        ps.setInt(++i, convertor.getDimensionIdByValue(statsLocationDimension.getLocationDiemension()));
        ps.setInt(++i, v.getUvs());
        ps.setInt(++i, v.getSessions());
        ps.setInt(++i, v.getBounceNumber());
        ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_FORMAT));
        ps.setInt(++i, v.getUvs());
        ps.setInt(++i, v.getSessions());
        ps.setInt(++i, v.getBounceNumber());
        //加载到bathc中
        ps.addBatch();
    }
}
