package edu.qianfeng.anlastic.mr.au;

import edu.qianfeng.anlastic.model.dim.StatsUserDimension;
import edu.qianfeng.anlastic.model.value.MapWritableValue;
import edu.qianfeng.anlastic.model.value.TimeOutputValue;
import edu.qianfeng.common.DateEnum;
import edu.qianfeng.common.KpiType;
import edu.qianfeng.util.TimeUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by lyd on 2018/6/1.
 * 活跃用户的reducer类   添加按小时统计活跃用户
 */
public class ActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    private static final Logger logger = Logger.getLogger(ActiveUserReducer.class);
    private Set<String> unique = new HashSet<String>(); //用于存储uuid，以便统计唯一的个数
    private MapWritableValue v = new MapWritableValue();

    //按小时统计所需属性
    private Map<Integer, Set<String>> hourlyUnique = new HashMap<Integer, Set<String>>();
    private MapWritable hourylyMap = new MapWritable();

    /**
     * 只在map执行之前执行一次
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //初始化hourlyUnique和hourylyMap
        for (int i = 0; i < 24; i++) {
            this.hourlyUnique.put(i, new HashSet<String>());
            this.hourylyMap.put(new IntWritable(i), new IntWritable(0));
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {

        //判断kpi是否为小时统计
        if (KpiType.HOURLY_ACTIVE_USER.kpiName.equals(key.getStatsCommon().getKpiDimension().getKpiName())) {
            //按小时计算
            for (TimeOutputValue tv : values) {
                String uuid = tv.getId();
                long time = tv.getTime();
                //获取小时数
                int hour = TimeUtil.getDateInfo(time, DateEnum.HOUR);
                //将uuid存储对应的小时(0-23)的set中
                this.hourlyUnique.get(hour).add(uuid);
            }
            //构造输出的value
            for (Map.Entry<Integer, Set<String>> entry : hourlyUnique.entrySet()) {
                this.hourylyMap.put(new IntWritable(entry.getKey()), new IntWritable(entry.getValue().size()));
            }
            this.v.setValue(this.hourylyMap);
            this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommon().getKpiDimension().getKpiName()));
            context.write(key, this.v);

        } else {
            //正常处理
            //将unitque中的数据清空
            this.unique.clear();
            for (TimeOutputValue tv : values) {
                //将uuid添加到unnique中,以便进行对统一个key进行去重uuid
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
}
