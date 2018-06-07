package edu.qianfeng.anlastic.mr.pv;

import edu.qianfeng.anlastic.model.dim.StatsUserDimension;
import edu.qianfeng.anlastic.model.value.MapWritableValue;
import edu.qianfeng.anlastic.model.value.TimeOutputValue;
import edu.qianfeng.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by lyd on 2018/6/1.
 * pageview的reducer类
 */
public class PageViewReducer extends Reducer<StatsUserDimension,TimeOutputValue,StatsUserDimension,MapWritableValue>{
    private static final Logger logger = Logger.getLogger(PageViewReducer.class);
    private MapWritableValue v = new MapWritableValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //将unitque中的数据清空
        int counter = 0;
        for (TimeOutputValue tv:values) {
            counter ++;
        }
        //构造输出的value
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1),new IntWritable(counter));
        this.v.setValue(map);

        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommon().getKpiDimension().getKpiName()));

        //最终输出
        context.write(key,this.v);
    }
}
