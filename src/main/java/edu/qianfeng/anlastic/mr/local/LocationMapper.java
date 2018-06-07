package edu.qianfeng.anlastic.mr.local;

import edu.qianfeng.anlastic.model.dim.StatsCommonDimension;
import edu.qianfeng.anlastic.model.dim.StatsLocationDimension;
import edu.qianfeng.anlastic.model.dim.base.*;
import edu.qianfeng.anlastic.model.value.TextOutputValue;
import edu.qianfeng.common.DateEnum;
import edu.qianfeng.common.EventLogConstant;
import edu.qianfeng.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by lyd on 2018/6/7.
 * 地域维度
 */
public class LocationMapper extends TableMapper<StatsLocationDimension,TextOutputValue>{
    private static final Logger logger = Logger.getLogger(LocationMapper.class);
    private byte[] family = Bytes.toBytes(EventLogConstant.LOG_FAMILY_NAME);
    private StatsLocationDimension k = new StatsLocationDimension();
    private TextOutputValue v = new TextOutputValue();
    private KpiDimension localKpi = new KpiDimension(KpiType.LOCATION.kpiName);


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //重hbase的result中获取统计该指标所需要的字段
        String serverTime = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstant.LOG_COLUMN_NAME_SERVER_TIME)));
        String uuid = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstant.LOG_COLUMN_NAME_UUID)));
        String sessionId = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstant.LOG_COLUMN_NAME_SESSION_ID)));
        String plaform = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstant.LOG_COLUMN_NAME_PLATFORM_NAME)));
        String country = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstant.LOG_COLUMN_NAME_COUNTRY)));
        String province = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstant.LOG_COLUMN_NAME_PROVINCE)));
        String city = Bytes.toString(value.getValue(family,Bytes.toBytes(EventLogConstant.LOG_COLUMN_NAME_CITY)));

        //判断该指标的必须字段不能为空
        if(StringUtils.isBlank(serverTime) || StringUtils.isBlank(uuid) || StringUtils.isEmpty(sessionId) || StringUtils.isBlank(plaform)){
            logger.warn("serverTime&&memberId&&plaform三者任意一个都不能为空:serverTime:"+serverTime+" uuid:"+uuid+" sessionId:"+sessionId+" platform:"+plaform);
            return;
        }

        //代码到这儿正常处理
        long longOfTime = Long.valueOf(serverTime);
        //设置map阶段输出的value的值
        this.v.setUuid(uuid);
        this.v.setSessionId(sessionId);

        //时间维度
        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY); //按天统计
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(plaform);
        List<LocationDiemension> locationDiemensions = LocationDiemension.buildList(country,province,city);

        //获取statsCommondimension
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        statsCommonDimension.setDateDimension(dateDimension);
        //设置kpi
        statsCommonDimension.setKpiDimension(localKpi);
        for (PlatformDimension pl:platformDimensions) {
            //设置pl
            statsCommonDimension.setPlatformDimension(pl);
            for (LocationDiemension local:locationDiemensions) {
                //覆盖kpi维度
//                statsCommonDimension.setKpiDimension(localKpi);
                this.k.setStatsCommonDimension(statsCommonDimension);
                this.k.setLocationDiemension(local);
                context.write(this.k,this.v);
            }
        }
    }
}
