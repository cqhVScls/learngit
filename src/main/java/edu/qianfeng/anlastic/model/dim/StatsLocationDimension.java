package edu.qianfeng.anlastic.model.dim;

import edu.qianfeng.anlastic.model.dim.base.BaseDimension;
import edu.qianfeng.anlastic.model.dim.base.LocationDiemension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lyd on 2018/6/6.
 * 用于地域维度的模块的map阶段输出的key的类型
 */
public class StatsLocationDimension extends StatsDimension{
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private LocationDiemension locationDiemension = new LocationDiemension();

    public StatsLocationDimension(){
    }

    public StatsLocationDimension(StatsCommonDimension statsCommonDimension, LocationDiemension locationDiemension) {
        this.statsCommonDimension = statsCommonDimension;
        this.locationDiemension = locationDiemension;
    }

    /**
     * 克隆当前对象的一个实例
     * @param dimension
     * @return
     */
    public static StatsLocationDimension clone(StatsLocationDimension dimension){
        StatsCommonDimension statsCommonDimension = StatsCommonDimension.clone(dimension.statsCommonDimension);
        LocationDiemension locationDiemension = new LocationDiemension(dimension.locationDiemension.getCountry(),
                dimension.locationDiemension.getProvince(),dimension.locationDiemension.getCity());
        return new StatsLocationDimension(statsCommonDimension,locationDiemension);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommonDimension.write(out);
        this.locationDiemension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommonDimension.readFields(in);
        this.locationDiemension.readFields(in);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(o == this){
            return 0;
        }
        StatsLocationDimension other = (StatsLocationDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if(tmp != 0){
            return tmp;
        }
        return this.locationDiemension.compareTo(other.locationDiemension);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsLocationDimension that = (StatsLocationDimension) o;

        if (statsCommonDimension != null ? !statsCommonDimension.equals(that.statsCommonDimension) : that.statsCommonDimension != null)
            return false;
        return locationDiemension != null ? locationDiemension.equals(that.locationDiemension) : that.locationDiemension == null;

    }

    @Override
    public int hashCode() {
        int result = statsCommonDimension != null ? statsCommonDimension.hashCode() : 0;
        result = 31 * result + (locationDiemension != null ? locationDiemension.hashCode() : 0);
        return result;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public LocationDiemension getLocationDiemension() {
        return locationDiemension;
    }

    public void setLocationDiemension(LocationDiemension locationDiemension) {
        this.locationDiemension = locationDiemension;
    }
}
