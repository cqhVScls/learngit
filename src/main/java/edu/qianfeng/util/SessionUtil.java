package edu.qianfeng.util;

/**
 * Created by lyd on 2018/6/6.
 * 专用于会话长度计算
 */
public class SessionUtil {
    private final static int sessionSize = 2;
    private long[] times; //用于存储最长和最短的时间戳
    private int index; //数组的索引
    private int size; //数组当前索引
    private long tmpTime; //临时时间

    /**
     * 传递一个session对应的时间戳
     *
     * @param time
     */
    public SessionUtil(long time) {
        this.times = new long[sessionSize];
        this.times[0] = time;
        this.index = 0;
        this.size = 1;
        this.tmpTime = 0;
    }

    /**
     * 添加时间戳
     *
     * @param time
     */
    public void addTime(long time) {
        tmpTime = this.times[0];
        if (this.times.length == 1) {
            if (tmpTime < time) {
                this.times[1] = time;
            } else {
                this.times[0] = time;
                this.times[1] = tmpTime;
            }
        }

        if (this.times.length == 2) {
            if (time < tmpTime) {
                this.times[0] = time;
            }
            if (time > tmpTime) {
                this.times[1] = time;
            }
            //代码走到这儿，time将会落在最大值和最小值之间
        }
    }

    public long getMinTime() {
        return this.times[0];
    }

    public long getMaxTime() {
        return this.times[1];
    }

    /**
     * 获取毫秒数
     *
     * @return
     */
    public long getTimeOfMill() {
        return getMaxTime() - getMinTime();
    }

    public long getTimeOfSecond() {
        return (getMaxTime() - getMinTime()) / 1000;
    }
}
