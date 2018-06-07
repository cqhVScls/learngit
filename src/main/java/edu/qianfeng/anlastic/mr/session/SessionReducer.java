package edu.qianfeng.anlastic.mr.session;

import edu.qianfeng.anlastic.model.dim.StatsUserDimension;
import edu.qianfeng.anlastic.model.value.MapWritableValue;
import edu.qianfeng.anlastic.model.value.TimeOutputValue;
import edu.qianfeng.common.GlobalConstants;
import edu.qianfeng.common.KpiType;
import edu.qianfeng.util.SessionUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lyd on 2018/6/6.
 * session个数和长度的reducer类
 */
public class SessionReducer extends Reducer<StatsUserDimension,TimeOutputValue,StatsUserDimension,MapWritableValue>{
    private static final Logger logger = Logger.getLogger(SessionReducer.class);

    //key=sessionId  value=sessionUtil对象
    Map<String,SessionUtil> sessionUtilMap = new HashMap<String,SessionUtil>();
    private MapWritableValue v = new MapWritableValue();
    private MapWritable map = new MapWritable();

    /**
     * 清空两个集合
     */
    private void startUp(){
        this.map.clear();
        this.sessionUtilMap.clear();
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        this.startUp(); //每一个维度的数据统计都需要事先清空map
        String  kpiName = key.getStatsCommon().getKpiDimension().getKpiName();
        if(KpiType.SESSION.kpiName.equals(kpiName)){
            //计算用户模块下的session个数和长度
            this.handleSessions(key,values,context);
        } else if(KpiType.BROWSER_SESSION.kpiName.equals(kpiName)){
            //计算浏览器模块下的session个数和长度
            this.handleSessions(key,values,context);
            //this.handleBrowserSessions(key,values,context);
        }
    }

    private void handleSessions(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException  {
        for (TimeOutputValue tv:values) {
            String sessionId = tv.getId();
            long time = tv.getTime();
            //获取sessionUtil
            SessionUtil sessionUtil = this.sessionUtilMap.get(sessionId);
            if(sessionUtil  == null){
                sessionUtil = new SessionUtil(time);
                //存储到集合中
                sessionUtilMap.put(sessionId,sessionUtil);
            }
            //代码走到这儿，代表sessionUtil里面肯定有值
            sessionUtil.addTime(time);
        }

        //计算长度和个数
        int sessionLength = 0;
        for (Map.Entry<String,SessionUtil> entry:sessionUtilMap.entrySet()) {
            long tmp = entry.getValue().getTimeOfMill();
            if(tmp < 0 || tmp > GlobalConstants.DAY_OF_MILLSECOND){
                continue;
            }
            sessionLength += tmp;
        }

        //计算时长时，当长度不为1秒，按1秒计算
        if(sessionLength %  1000 == 0){
            sessionLength = sessionLength / 1000;
        } else {
            sessionLength = sessionLength / 1000 + 1;
        }

        //构造输出的value的值
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommon().getKpiDimension().getKpiName()));
        //设置value的值
        this.map.put(new IntWritable(-1),new IntWritable(this.sessionUtilMap.size()));
        this.map.put(new IntWritable(-2),new IntWritable(sessionLength));
        this.v.setValue(this.map);
        //最终输出
        context.write(key,this.v);
    }

    private void handleBrowserSessions(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) {

    }


}
