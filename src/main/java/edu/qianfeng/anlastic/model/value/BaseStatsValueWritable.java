package edu.qianfeng.anlastic.model.value;

import edu.qianfeng.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * Created by lyd on 2018/5/31.
 * 用于封装mapreduce中输出value的值的类型的顶级父类
 */
public abstract class BaseStatsValueWritable implements Writable {
    public abstract KpiType getKpi();
}
