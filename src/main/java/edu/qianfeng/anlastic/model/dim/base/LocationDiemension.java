package edu.qianfeng.anlastic.model.dim.base;

import edu.qianfeng.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lyd on 2018/6/6.
 * 地域维度类
 */
public class LocationDiemension extends BaseDimension {
    private int id;
    private String country;
    private String province;
    private String city;

    public LocationDiemension() {
    }

    public LocationDiemension(String country, String province, String city) {
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public LocationDiemension(int id, String country, String province, String city) {
        this(country, province, city);
        this.id = id;
    }

    /**
     * 返回地域维度的集合对象
     *
     * @param country
     * @param province
     * @param city
     * @return
     */
    public static List<LocationDiemension> buildList(String country, String province, String city) {
        if (StringUtils.isEmpty(country)) {
            country = province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(province)) {
            province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(city)) {
            city = GlobalConstants.DEFAULT_VALUE;
        }

        List<LocationDiemension> li = new ArrayList<LocationDiemension>();
        li.add(new LocationDiemension(country, province, city));
        li.add(new LocationDiemension(country, province, GlobalConstants.ALL_OF_VALUE));
        return li;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.country);
        out.writeUTF(this.province);
        out.writeUTF(this.city);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.country = in.readUTF();
        this.province = in.readUTF();
        this.city = in.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        LocationDiemension other = (LocationDiemension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.country.compareTo(other.country);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.province.compareTo(other.province);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.city.compareTo(other.city);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocationDiemension that = (LocationDiemension) o;

        if (id != that.id) return false;
        if (country != null ? !country.equals(that.country) : that.country != null) return false;
        if (province != null ? !province.equals(that.province) : that.province != null) return false;
        return city != null ? city.equals(that.city) : that.city == null;

    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + (province != null ? province.hashCode() : 0);
        result = 31 * result + (city != null ? city.hashCode() : 0);
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}