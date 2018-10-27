package edu.qianfeng.anlastic.mr.am;

import edu.qianfeng.anlastic.model.dim.StatsUserDimension;
import edu.qianfeng.anlastic.model.value.MapWritableValue;
import edu.qianfeng.anlastic.model.value.TimeOutputValue;
import edu.qianfeng.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by lyd on 2018/6/1.
 * 活跃会员的reducer类
 */
public class ActiveMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    private static final Logger logger = Logger.getLogger(ActiveMemberReducer.class);
    private Set<String> unique = new HashSet<String>(); //用于存储umid，以便统计唯一的个数
    private MapWritableValue v = new MapWritableValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //将unitque中的数据清空
        this.unique.clear();
        for (TimeOutputValue tv : values) {
            //将uuid添加到unnique中,以便进行对统一个key进行去重umid
            this.unique.add(tv.getId());
        }
        //构造输出的value
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        this.v.setValue(map);
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommon().getKpiDimension().getKpiName()));
        //最终输出
        context.write(key, this.v);
    }
}
