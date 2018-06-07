package edu.qianfeng.anlastic.mr.local;

import edu.qianfeng.anlastic.model.dim.StatsLocationDimension;
import edu.qianfeng.anlastic.model.value.LocationOutputWritableValue;
import edu.qianfeng.anlastic.model.value.TextOutputValue;
import edu.qianfeng.common.KpiType;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by lyd on 2018/6/7.
 * 地域模块的reducer类
 */
public class LocationReducer extends Reducer<StatsLocationDimension,TextOutputValue,StatsLocationDimension,LocationOutputWritableValue>{
    private static final Logger logger = Logger.getLogger(LocationReducer.class);
    private Set<String> uvsunique = new HashSet<String>(); //用于存储uuid，以便统计唯一的个数
    private Map<String,Integer> sessions = new HashMap<String,Integer>();
    private LocationOutputWritableValue v = new LocationOutputWritableValue();

    @Override
    protected void reduce(StatsLocationDimension key, Iterable<TextOutputValue> values, Context context) throws IOException, InterruptedException {

        try {
            for (TextOutputValue tv:values) {
                String uuid = tv.getUuid();
                String sessionId = tv.getSessionId();

                this.uvsunique.add(uuid);
                if(this.sessions.containsKey(sessionId)){
                    this.sessions.put(sessionId,2);
                } else {
                    this.sessions.put(sessionId,1);
                }
            }

            //设置输出的值
            this.v.setUvs(this.uvsunique.size());
            this.v.setSessions(this.sessions.size());
            //计算跳出会话个数
            int bounceNum = 0;
            for (Map.Entry<String,Integer> entry:sessions.entrySet()) {
                if(entry.getValue() == 1){
                    bounceNum ++;
                }
            }
            //设置kpi
            this.v.setKpiType(KpiType.LOCATION);
//            this.v.setKpiType(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
            this.v.setBounceNumber(bounceNum);
            //输出
            context.write(key,this.v);
        } finally {
            //将unitque中的数据清空
            this.uvsunique.clear();
            this.sessions.clear();
        }
    }
}
