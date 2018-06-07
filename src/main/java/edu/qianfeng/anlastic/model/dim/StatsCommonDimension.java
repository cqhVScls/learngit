package edu.qianfeng.anlastic.model.dim;

import edu.qianfeng.anlastic.model.dim.base.BaseDimension;
import edu.qianfeng.anlastic.model.dim.base.DateDimension;
import edu.qianfeng.anlastic.model.dim.base.KpiDimension;
import edu.qianfeng.anlastic.model.dim.base.PlatformDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lyd on 2018/5/31.
 */
public class StatsCommonDimension extends StatsDimension{
    private DateDimension dateDimension = new DateDimension();
    private PlatformDimension platformDimension = new PlatformDimension();
    private KpiDimension kpiDimension = new KpiDimension();

    public StatsCommonDimension(){
    }

    public StatsCommonDimension(DateDimension dateDimension, PlatformDimension platformDimension, KpiDimension kpiDimension) {
        this.dateDimension = dateDimension;
        this.platformDimension = platformDimension;
        this.kpiDimension = kpiDimension;
    }

    /**
     * 克隆一个当前类的实例
     * @param dimension
     * @return
     */
    public static StatsCommonDimension clone(StatsCommonDimension dimension){
        DateDimension date = new DateDimension(dimension.dateDimension.getYear(),
                dimension.dateDimension.getSeason(),dimension.dateDimension.getMonth(),
                dimension.dateDimension.getWeek(),dimension.dateDimension.getDay(),
                dimension.dateDimension.getType(),dimension.dateDimension.getCalendar());
        PlatformDimension platform = new PlatformDimension(dimension.platformDimension.getPlatformName());
        KpiDimension kpi = new KpiDimension(dimension.kpiDimension.getKpiName());
        return new StatsCommonDimension(date,platform,kpi);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.dateDimension.write(out); //对象序列化
        this.platformDimension.write(out);
        this.kpiDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dateDimension.readFields(in);
        this.platformDimension.readFields(in);
        this.kpiDimension.readFields(in);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }
        StatsCommonDimension other = (StatsCommonDimension) o;
        int tmp = this.dateDimension.compareTo(other.dateDimension);
        if(tmp != 0){
            return tmp;
        }
        tmp = this.platformDimension.compareTo(other.platformDimension);
        if(tmp != 0){
            return tmp;
        }
        return this.kpiDimension.compareTo(other.kpiDimension);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsCommonDimension that = (StatsCommonDimension) o;

        if (dateDimension != null ? !dateDimension.equals(that.dateDimension) : that.dateDimension != null)
            return false;
        if (platformDimension != null ? !platformDimension.equals(that.platformDimension) : that.platformDimension != null)
            return false;
        return kpiDimension != null ? kpiDimension.equals(that.kpiDimension) : that.kpiDimension == null;

    }

    @Override
    public int hashCode() {
        int result = dateDimension != null ? dateDimension.hashCode() : 0;
        result = 31 * result + (platformDimension != null ? platformDimension.hashCode() : 0);
        result = 31 * result + (kpiDimension != null ? kpiDimension.hashCode() : 0);
        return result;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public PlatformDimension getPlatformDimension() {
        return platformDimension;
    }

    public void setPlatformDimension(PlatformDimension platformDimension) {
        this.platformDimension = platformDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }
}
