package edu.qianfeng.anlastic.mr;

import edu.qianfeng.anlastic.model.dim.base.BaseDimension;
import edu.qianfeng.anlastic.model.value.BaseStatsValueWritable;
import edu.qianfeng.anlastic.service.IDimensionConvertor;
import edu.qianfeng.anlastic.service.impl.IDimensionConvertorImpl;
import edu.qianfeng.common.GlobalConstants;
import edu.qianfeng.common.KpiType;
import edu.qianfeng.util.JDBCUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lyd on 2018/6/1.
 * 自定义mysql的输出格式
 */
public class AnlasticOutputFormat extends OutputFormat<BaseDimension, BaseStatsValueWritable> {
    private static final Logger logger = Logger.getLogger(AnlasticOutputFormat.class);

    /**
     * 用于封装 recordWriter
     */
    public static class AnlasticRecordWriter extends RecordWriter<BaseDimension, BaseStatsValueWritable> {
        private Connection conn = null;
        private Configuration conf = null;
        private IDimensionConvertor convert = null;
        //存储指标-ps
        private Map<KpiType, PreparedStatement> cache = new HashMap<KpiType, PreparedStatement>();
        //指标-数量
        private Map<KpiType, Integer> batch = new HashMap<KpiType, Integer>();

        public AnlasticRecordWriter() {
        }

        public AnlasticRecordWriter(Connection conn, Configuration conf, IDimensionConvertor convert) {
            this.conn = conn;
            this.conf = conf;
            this.convert = convert;
        }

        /**
         * 写数据
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(BaseDimension key, BaseStatsValueWritable value) throws IOException, InterruptedException {
            if (key == null || value == null) {
                return;
            }

            try {
                PreparedStatement ps = null;
                int count = 0;
                KpiType kpi = value.getKpi();
                //判断cache中是否有对应的ps
                if (this.cache.containsKey(kpi)) {
                    ps = this.cache.get(kpi);
                    count = this.batch.get(kpi);
                } else {
                    ps = conn.prepareStatement(conf.get(kpi.kpiName));  //sql存储到conf中
                    //install_user "insert into ..........."
                    //将其设置缓存中
                    this.cache.put(kpi, ps);
                }

                //不管是否有还是没有，都将批量batch更新
                count++;
                batch.put(kpi, count);

                //为每一个redcuce的对应的kpi指标的ps进行赋值
                //collector_install_user edu.qianfeng.anlastic.mr.....;
                String collectorName = conf.get(GlobalConstants.OUTPUT_COLLECTOR_PREFIX + kpi.kpiName);
                Class<?> classz = Class.forName(collectorName);  //将collectorName这一串 字符串转换成对应的类
                IOuputCollector collector = (IOuputCollector) classz.newInstance();
                //使用IOuputCollector接口对对应的kpi进行赋值
                collector.collect(conf, key, value, ps, convert);

                //判断batch是否可以批量执行
                if (count % GlobalConstants.BATCH_DEFULT_NUMBER == 0) {
                    ps.executeBatch();
                    //conn.commit();
                    batch.remove(kpi);
                }

            } catch (SQLException e) {
                logger.warn("为reducer的指标对应的sql赋值时sql异常", e);
            } catch (ClassNotFoundException e) {
                logger.warn("为reducer的指标对应的sql赋值时类为未现异常", e);
            } catch (InstantiationException e) {
                logger.warn("为reducer的指标对应的sql赋值时获取实例异常", e);
            } catch (IllegalAccessException e) {
                logger.warn("为reducer的指标对应的sql赋值时非法处理异常", e);
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try {
                for (Map.Entry<KpiType, PreparedStatement> entry : this.cache.entrySet()) {
                    entry.getValue().executeBatch();
                }
            } catch (SQLException e) {
                logger.warn("在关闭mysql相关对象时只能够executbatch方法异常", e);
            } finally {
                if (conn != null) {
                    try {
                        conn.commit();
                    } catch (SQLException e) {
                        //do nothning
                    } finally {
                        for (Map.Entry<KpiType, PreparedStatement> entry : this.cache.entrySet()) {
                            JDBCUtil.close(null, entry.getValue(), null);
                        }
                        //最后关闭掉conn
                        JDBCUtil.close(conn, null, null);
                    }
                }
            }
        }
    }


    @Override
    public RecordWriter<BaseDimension, BaseStatsValueWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Connection conn = JDBCUtil.getconn();
        IDimensionConvertor convertor = new IDimensionConvertorImpl();
        return new AnlasticRecordWriter(conn, conf, convertor);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        //do nothing  输出到mysql的表不用检测即可
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }
}