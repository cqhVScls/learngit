package edu.qianfeng.etl.tohbase;

import edu.qianfeng.common.EventLogConstant;
import edu.qianfeng.common.GlobalConstants;
import edu.qianfeng.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by lyd on 2018/5/30.
 * 存储到hbase的runner类
 */
public class LogDAtaToHbaseRunner implements Tool {
    public static final Logger logger = Logger.getLogger(LogDAtaToHbaseRunner.class);
    private Configuration conf = new Configuration();

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.conf.addResource("hbase-site.xml");
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /**
     * 驱动
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //设置参数
        this.setArgs(args,conf);
        //判断hbase的表是否存在
        this.isExistsHbaseTable(conf);
        //获取job
        Job job = Job.getInstance(conf,"to_hbase");
        job.setJarByClass(LogDAtaToHbaseRunner.class);

        job.setMapperClass(LogDataToHbaseMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);
        //初始化Reducer
        //本地提交本地运行  addDependencyJars ： false 则本地运行
        TableMapReduceUtil.initTableReducerJob(EventLogConstant.LOG_HBASE_NAME,
                null,job,null,null,null,null,false);

        //本地提交集群运行  addDependencyJars： true 则集群运行
//        TableMapReduceUtil.initTableReducerJob(EventLogConstant.LOG_HBASE_NAME,
//                null,job,null,null,null,null,true);
        this.setInputPath(job);  //设置job的输入路径
        return job.waitForCompletion(true)?0:1;
    }

    /**
     * 数据被采集到hdfs中的     /logs/05/30
     * @param job
     */
    private void setInputPath(Job job) {
        Configuration conf = job.getConfiguration();
        String date = conf.get(GlobalConstants.RUNNING_DATE_FORMAT); //date=2018-05-30
        String[] dates = date.split("-");
        Path path = new Path("/logs/"+dates[1]+"/"+dates[2]);
        try {
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(path)){
                FileInputFormat.addInputPath(job,path);
            } else {
                throw new RuntimeException("需要处理的数据文件不存在：path:"+path.toString());
            }
        } catch (IOException e) {
            logger.warn("设置处理数据的路径异常",e);
        }
    }

    /**
     * 判断hbase的表是否存在，不存在则创建
     * @param conf
     */
    private void isExistsHbaseTable(Configuration conf) {
        HBaseAdmin admin = null;
        try {
             admin = new HBaseAdmin(conf);
            TableName tn = TableName.valueOf(EventLogConstant.LOG_HBASE_NAME);
            HTableDescriptor hdc = new HTableDescriptor(tn);
            //判断hdc是否存在
            if(!admin.tableExists(tn)){
                //获取一个列簇
                HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(EventLogConstant.LOG_FAMILY_NAME));
                //需要将列簇添加到表
                hdc.addFamily(hcd);
                //admin提交创建
                admin.createTable(hdc);
            }
        } catch (IOException e) {
            logger.warn("获取hbaseAdmin对象异常",e);
        } finally {
            if(admin  != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    //do nothing
                }
            }
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
     * 主函数
     * @param args
     */
    public static void main(String[] args) {
        try {
           int isok = ToolRunner.run(new Configuration(),new LogDAtaToHbaseRunner(),args);
            System.exit(isok);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
