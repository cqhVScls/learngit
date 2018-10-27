package edu.qianfeng.anlastic.model.value;

import edu.qianfeng.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lyd on 2018/6/7.
 * 地域模块的map端输出的value的类
 */
public class TextOutputValue extends BaseStatsValueWritable {
    private String uuid;
    private String sessionId;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.uuid);
        out.writeUTF(this.sessionId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uuid = in.readUTF();
        this.sessionId = in.readUTF();
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
