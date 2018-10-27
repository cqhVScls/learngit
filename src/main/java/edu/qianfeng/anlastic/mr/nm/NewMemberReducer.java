package edu.qianfeng.anlastic.mr.nm;

import edu.qianfeng.anlastic.model.dim.StatsUserDimension;
import edu.qianfeng.anlastic.model.value.MapWritableValue;
import edu.qianfeng.anlastic.model.value.TimeOutputValue;
import edu.qianfeng.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by lyd on 2018/6/1.
 * 新增会员的reducer类
 */
public class NewMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    private static final Logger logger = Logger.getLogger(NewMemberReducer.class);
    private Set<String> unique = new HashSet<String>(); //用于存储memberId，以便统计唯一的个数
    private MapWritableValue v = new MapWritableValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //将unitque中的数据清空
        this.unique.clear();


        for (TimeOutputValue tv : values) {
            //将memberId添加到unnique中,以便进行对统一个key进行去重memberId
            this.unique.add(tv.getId());
        }

        //为v设置kpi为member_info，目的将该memberId存储。必须对unique进行循环
        this.v.setKpi(KpiType.MEMBER_INFO);
        //构造输出的value
        MapWritable map = new MapWritable();
        for (String memberId : unique) {
            map.put(new IntWritable(-1), new Text(memberId));
            this.v.setValue(map);
            context.write(key, this.v); //该语句用于相同维度的有多个新增会员id时，循环存储的辅助表中
        }

        //注意需要清除map
        map.clear();
        map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        this.v.setValue(map);

        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommon().getKpiDimension().getKpiName()));

        //最终输出
        context.write(key, this.v);
    }
}
