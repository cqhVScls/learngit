package edu.qianfeng.anlastic.mr.au;

import com.google.common.collect.Lists;
import edu.qianfeng.anlastic.model.dim.StatsUserDimension;
import edu.qianfeng.anlastic.model.value.MapWritableValue;
import edu.qianfeng.anlastic.model.value.TimeOutputValue;
import edu.qianfeng.anlastic.mr.AnlasticOutputFormat;
import edu.qianfeng.common.EventLogConstant;
import edu.qianfeng.common.GlobalConstants;
import edu.qianfeng.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * 新增用户的驱动类
 * Created by lyd on 2018/6/1.
 */
public class ActiveUserRunner implements Tool{
    private static final Logger logger = Logger.getLogger(ActiveUserRunner.class);
    private Configuration conf = new Configuration();

    /**
     * 主函数
     * @param args
     */
    public static void main(String[] args) {
        try {
            int isok = ToolRunner.run(new Configuration(),new ActiveUserRunner(),args);
            System.exit(isok);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setConf(Configuration conf) {
        //设置需要的配置文件
        conf.addResource("query-mapping.xml");
        conf.addResource("output-mapping.xml");
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //设置参数
        this.setArgs(args,conf);
        //获取job
        Job job = Job.getInstance(conf,"new_install_user");
        job.setJarByClass(ActiveUserRunner.class);

        //初始化TableMapper  本地提交本地运行
        TableMapReduceUtil.initTableMapperJob(this.listScans(job),ActiveUserMapper.class,
                StatsUserDimension.class,TimeOutputValue.class,job,false);

        //初始化TableMapper  本地提交集群运行
//        TableMapReduceUtil.initTableMapperJob(this.listScans(job),NewInstallUserMapper.class,
//                StatsUserDimension.class,TimeOutputValue.class,job,true);

        job.setReducerClass(ActiveUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        //设置输出格式
        job.setOutputFormatClass(AnlasticOutputFormat.class);

        if(job.waitForCompletion(true)){
            return 0;
        } else {
            return 1;
        }
    }


    /**
     * 设置参数 yarn jar /home/*.jar *.LogDAtaToHbaseRunner -d 2018-05-30  （/05/30/*）
     * @param args
     */
    private void setArgs(String[] args,Configuration conf) {
        String date = null;
        //循环参数列表
        for (int i=0;i<args.length;i++) {
            if(args[i].equals("-d")){
                if(i+1 < args.length){
                    date = args[i+1];
                    break;
                }
            }
        }
        //判断取出来的date是否为空，为空则默认使用昨天的日期
        if(StringUtils.isEmpty(date) || !TimeUtil.isValidRunningDate(date)){
            date = TimeUtil.getYesterday();
        }
        //然后将date设置到conf中
        conf.set(GlobalConstants.RUNNING_DATE_FORMAT,date); //
    }

    /**
     * 重hbase中获取数据
     * @param job
     * @return
     */
    private List<Scan> listScans(Job job) {
        Configuration conf = job.getConfiguration();
        //获取运行的日期
        long startDate = TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE_FORMAT));
        long endDate = startDate + GlobalConstants.DAY_OF_MILLSECOND;
        //定义一个扫描器
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startDate+""));
        scan.setStopRow(Bytes.toBytes(endDate+""));

        //定义一个过滤器,只过滤launch的事件出来
        FilterList fl = new FilterList();

        //设置需要扫描出来的字段
        String columns[] = new String[]{
                EventLogConstant.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstant.LOG_COLUMN_NAME_UUID,
                EventLogConstant.LOG_COLUMN_NAME_PLATFORM_NAME,
                EventLogConstant.LOG_COLUMN_NAME_BROWSER_NAME,
                EventLogConstant.LOG_COLUMN_NAME_BROWSER_VERSION
        };
        fl.addFilter(this.getColumnList(columns));
        //设置hbase扫描的表名
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
                Bytes.toBytes(EventLogConstant.LOG_HBASE_NAME));
        //将过滤器添加扫描其中
        scan.setFilter(fl);
        return Lists.newArrayList(scan);
    }

    /**
     *多列值前缀过滤器
     * @param columns
     * @return
     */
    private Filter getColumnList(String[] columns) {
        int length = columns.length;
        byte[][] filters = new byte[length][];

        for (int i = 0;i < length;i++) {
            filters[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(filters);
    }
}