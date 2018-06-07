package edu.qianfeng.anlastic.mr.nm;

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
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by lyd on 2018/6/4.
 * 为新会员的sql进行赋值
 */
public class NewMemberCollector implements IOuputCollector{
    @Override
    public void collect(Configuration conf, BaseDimension key,
                        BaseStatsValueWritable value, PreparedStatement ps,
                        IDimensionConvertor convertor) throws IOException, SQLException {
        StatsUserDimension statsUserDimension = (StatsUserDimension)key;
        int i = 0;
        KpiType kpi = value.getKpi();
        switch (kpi){
            case NEW_MEMBER:
                IntWritable newInstallUsers = (IntWritable)((MapWritableValue)value).getValue().get(new IntWritable(-1));
                //设置值
                ps.setInt(++i,convertor.getDimensionIdByValue(((StatsUserDimension) key).getStatsCommon().getDateDimension()));
                ps.setInt(++i,convertor.getDimensionIdByValue(((StatsUserDimension) key).getStatsCommon().getPlatformDimension()));
                ps.setInt(++i,newInstallUsers.get());
                ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_FORMAT));
                ps.setInt(++i,newInstallUsers.get());
                break;
            case BROWSER_NEW_MEMBER:
                newInstallUsers = (IntWritable)((MapWritableValue)value).getValue().get(new IntWritable(-1));
                //设置值
                ps.setInt(++i,convertor.getDimensionIdByValue(((StatsUserDimension) key).getStatsCommon().getDateDimension()));
                ps.setInt(++i,convertor.getDimensionIdByValue(((StatsUserDimension) key).getStatsCommon().getPlatformDimension()));
                ps.setInt(++i,convertor.getDimensionIdByValue(((StatsUserDimension) key).getBrowser()));
                ps.setInt(++i,newInstallUsers.get());
                ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_FORMAT));
                ps.setInt(++i,newInstallUsers.get());
                break;

            case MEMBER_INFO:
                Text memberId = (Text) ((MapWritableValue)value).getValue().get(new IntWritable(-1));
                ps.setString(++i,memberId.toString());
                ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_FORMAT));
                ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_FORMAT));
                ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE_FORMAT));
                break;

            default:
              throw  new RuntimeException("该类型的kpi暂时不支持赋值.");
            }
            //加载到bathc中
            ps.addBatch();
    }
}
