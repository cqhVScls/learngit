package edu.qianfeng.anlastic.mr.nu;

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
 * 新增用户的reducer类
 */
public class NewInstallUserReducer extends Reducer<StatsUserDimension,TimeOutputValue,StatsUserDimension,MapWritableValue>{
    private static final Logger logger = Logger.getLogger(NewInstallUserReducer.class);
    private Set<String> unique = new HashSet<String>(); //用于存储uuid，以便统计唯一的个数
    private MapWritableValue v = new MapWritableValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //将unitque中的数据清空
        this.unique.clear();
        for (TimeOutputValue tv:values) {
            //将uuid添加到unnique中,以便进行对统一个key进行去重uuid
            this.unique.add(tv.getId());
        }
        //构造输出的value
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.v.setValue(map);

        //获取kpi
        /*String kpiName = key.getStatsCommon().getKpiDimension().getKpiName();
        if(kpiName.equals(KpiType.NEW_INSTALL_USER.kpiName)){
            //为v设置kpi
            this.v.setKpi(KpiType.NEW_INSTALL_USER);
        } else if(kpiName.equals(KpiType.BROWSER_NEW_INSTALL_USER.kpiName)){
            //为v设置kpi
            this.v.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
        }*/
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommon().getKpiDimension().getKpiName()));

        //最终输出
        context.write(key,this.v);
    }
}
