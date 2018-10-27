package edu.qianfeng.anlastic.model.dim;

import edu.qianfeng.anlastic.model.dim.base.BaseDimension;
import edu.qianfeng.anlastic.model.dim.base.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lyd on 2018/5/31.
 * 用户模块的输出key数据类型
 */
public class StatsUserDimension extends StatsDimension {
    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    private BrowserDimension browser = new BrowserDimension();

    public StatsUserDimension() {
    }

    public StatsUserDimension(StatsCommonDimension statsCommonDimension, BrowserDimension browserDimension) {
        this.statsCommon = statsCommonDimension;
        this.browser = browserDimension;
    }

    /**
     * 克隆当前对象一个实例
     *
     * @param dimension
     * @return
     */
    public static StatsUserDimension clone(StatsUserDimension dimension) {
        StatsCommonDimension statsCommon = StatsCommonDimension.clone(dimension.statsCommon);
        BrowserDimension browserDimension = new BrowserDimension(dimension.browser.getBrowserName(),
                dimension.browser.getBrowserVersion());
        return new StatsUserDimension(statsCommon, browserDimension);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommon.write(out);
        this.browser.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommon.readFields(in);
        this.browser.readFields(in);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.statsCommon.compareTo(other.statsCommon);
        if (tmp != 0) {
            return tmp;
        }
        return this.browser.compareTo(other.browser);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsUserDimension that = (StatsUserDimension) o;

        if (statsCommon != null ? !statsCommon.equals(that.statsCommon) : that.statsCommon != null) return false;
        return browser != null ? browser.equals(that.browser) : that.browser == null;

    }

    @Override
    public int hashCode() {
        int result = statsCommon != null ? statsCommon.hashCode() : 0;
        result = 31 * result + (browser != null ? browser.hashCode() : 0);
        return result;
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public BrowserDimension getBrowser() {
        return browser;
    }

    public void setBrowser(BrowserDimension browser) {
        this.browser = browser;
    }
}
