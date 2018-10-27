package edu.qianfeng.anlastic.model.value;

import edu.qianfeng.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lyd on 2018/5/31.
 * reduce阶段输出的value类型
 */
public class MapWritableValue extends BaseStatsValueWritable {
    private MapWritable value = new MapWritable();
    private KpiType kpi;

    public MapWritableValue() {
    }

    public MapWritableValue(MapWritable value, KpiType kpi) {
        this.value = value;
        this.kpi = kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.value.write(out);  //注意mapWritable的类型的序列化
        WritableUtils.writeEnum(out, this.kpi); //枚举类型的序列化
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.value.readFields(in);
        WritableUtils.readEnum(in, KpiType.class);
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }
}
