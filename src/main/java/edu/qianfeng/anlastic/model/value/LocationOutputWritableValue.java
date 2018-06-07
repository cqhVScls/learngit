package edu.qianfeng.anlastic.model.value;

import edu.qianfeng.common.KpiType;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lyd on 2018/6/6.
 * 用于地域模块的reducer的输出
 */
public class LocationOutputWritableValue extends BaseStatsValueWritable{
    private KpiType kpiType;
    private int uvs;//活跃用户
    private int sessions;//session个数
    private int bounceNumber;//跳出会话个数


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.uvs);
        out.writeInt(this.sessions);
        out.writeInt(this.bounceNumber);
        WritableUtils.writeEnum(out,this.kpiType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uvs = in.readInt();
        this.sessions = in.readInt();
        this.bounceNumber = in.readInt();
        WritableUtils.readEnum(in,KpiType.class);
    }

    @Override
    public KpiType getKpi() {
        return this.kpiType;
    }

    public void setKpiType(KpiType kpiType) {
        this.kpiType = kpiType;
    }

    public int getUvs() {
        return uvs;
    }

    public void setUvs(int uvs) {
        this.uvs = uvs;
    }

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
    }

    public int getBounceNumber() {
        return bounceNumber;
    }

    public void setBounceNumber(int bounceNumber) {
        this.bounceNumber = bounceNumber;
    }
}
