package edu.qianfeng.anlastic.model.value;

import edu.qianfeng.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lyd on 2018/5/31.
 * map阶段输出的value类型
 */
public class TimeOutputValue extends BaseStatsValueWritable{
    private String id; //该id可以泛指u_ud u_mid u_sd
    private long time;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.time = in.readLong();
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return this.time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
