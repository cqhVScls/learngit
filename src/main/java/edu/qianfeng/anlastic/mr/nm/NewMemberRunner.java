package edu.qianfeng.anlastic.mr.nm;

import com.google.common.collect.Lists;
import edu.qianfeng.anlastic.model.dim.StatsUserDimension;
import edu.qianfeng.anlastic.model.dim.base.DateDimension;
import edu.qianfeng.anlastic.model.value.MapWritableValue;
import edu.qianfeng.anlastic.model.value.TimeOutputValue;
import edu.qianfeng.anlastic.mr.AnlasticOutputFormat;
import edu.qianfeng.anlastic.mr.nu.NewInstallUserMapper;
import edu.qianfeng.anlastic.mr.nu.NewInstallUserReducer;
import edu.qianfeng.common.DateEnum;
import edu.qianfeng.common.EventLogConstant;
import edu.qianfeng.common.GlobalConstants;
import edu.qianfeng.util.JDBCUtil;
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

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 新增会员和新增 总会员的驱动类
 * Created by lyd on 2018/6/1.
 */
public class NewMemberRunner implements Tool{
    private static final Logger logger = Logger.getLogger(NewMemberRunner.class);
    private Configuration conf = new Configuration();

    /**
     * 主函数
     * @param args
     */
    public static void main(String[] args) {
        try {
            int isok = ToolRunner.run(new Configuration(),new NewMemberRunner(),args);
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
        job.setJarByClass(NewMemberRunner.class);

        //初始化TableMapper  本地提交本地运行
        TableMapReduceUtil.initTableMapperJob(this.listScans(job),NewMemberMapper.class,
                StatsUserDimension.class,TimeOutputValue.class,job,false);

        //初始化TableMapper  本地提交集群运行
//        TableMapReduceUtil.initTableMapperJob(this.listScans(job),NewInstallUserMapper.class,
//                StatsUserDimension.class,TimeOutputValue.class,job,true);

        job.setReducerClass(NewMemberReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        //设置输出格式
        job.setOutputFormatClass(AnlasticOutputFormat.class);

//        return job.waitForCompletion(true)?0:1;
        if(job.waitForCompletion(true)){
            this.calculateTotalMember(job);  //计算新增总会员
            return 0;
        } else {
            return 1;
        }
    }


    /**
     * 计算新增总用户
     * @param job
     */
    private void calculateTotalMember(Job job) {
        Configuration conf = job.getConfiguration();
        long nowDate = TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE_FORMAT));
        long yesterdayDate = nowDate - GlobalConstants.DAY_OF_MILLSECOND;
        DateDimension nowDateDimension = DateDimension.buildDate(nowDate, DateEnum.DAY);
        DateDimension yesterdayDateDimension = DateDimension.buildDate(yesterdayDate, DateEnum.DAY);

        //获取时间维度的Id
        int nowDateDimensionId = 0;
        int yesterdayDateDimensionId = 0;

        Connection conn = JDBCUtil.getconn();
        nowDateDimensionId = this.getDateDimensionId(conn,nowDateDimension); //获取当日时间维度id
        yesterdayDateDimensionId = this.getDateDimensionId(conn,yesterdayDateDimension); //获取当日时间维度id

        //如果昨天的时间维度id大于0则获取总用户数据
        Map<String,Integer> map = new HashMap<String,Integer>();  //key是维度集合，value是指标
        PreparedStatement  ps = null;
        ResultSet rs = null;
        try {
            //================用户模块的新增总用户start===========
            if(yesterdayDateDimensionId > 0){
                ps = conn.prepareStatement("select `platform_dimension_id`,`total_members` from `stats_user` where `date_dimension_id` = ?");
                ps.setInt(1,yesterdayDateDimensionId);
                rs = ps.executeQuery();
                while (rs.next()){
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalUser = rs.getInt("total_members");
                    map.put(platformId+"",totalUser);
                }
            }
            //获取统计当日的新增用户数
            if(nowDateDimensionId > 0){
                ps = conn.prepareStatement("select `platform_dimension_id`,`new_members` from `stats_user` where `date_dimension_id` = ?");
                ps.setInt(1,nowDateDimensionId);
                rs = ps.executeQuery();
                while (rs.next()){
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalUser = rs.getInt("new_members");
                    if(map.containsKey(platformId+"")){
                        totalUser +=  map.get(platformId+"");
                    }
                    map.put(platformId+"",totalUser);
                }
            }
            //更新用户模块新增总用户数据
            ps = conn.prepareStatement("insert into `stats_user`(`date_dimension_id`,`platform_dimension_id`,`total_members`) values(?,?,?) on duplicate key update `total_members` = ?");
            //循环map
            for (Map.Entry<String,Integer> entry:map.entrySet()) {
               ps.setInt(1,nowDateDimensionId);
                ps.setInt(2,Integer.parseInt(entry.getKey()));
                ps.setInt(3,entry.getValue());
                ps.setInt(4,entry.getValue());
                ps.executeUpdate();
            }
            //================用户模块的新增总用户end===========


            //================用浏览器模块的新增总用户===========
            map.clear();
            if(yesterdayDateDimensionId > 0){
                ps = conn.prepareStatement("select `platform_dimension_id`,`browser_dimension_id`,`total_members` from `stats_device_browser` where `date_dimension_id` = ?");
                ps.setInt(1,yesterdayDateDimensionId);
                rs = ps.executeQuery();
                while (rs.next()){
                    int platformId = rs.getInt("platform_dimension_id");
                    int browserId = rs.getInt("browser_dimension_id");
                    int totalUser = rs.getInt("total_members");
                    map.put(platformId+"_"+browserId,totalUser);
                }
            }
            //获取统计当日的新增用户数
            if(nowDateDimensionId > 0){
                ps = conn.prepareStatement("select `platform_dimension_id`,`browser_dimension_id`,`new_members` from `stats_device_browser` where `date_dimension_id` = ?");
                ps.setInt(1,nowDateDimensionId);
                rs = ps.executeQuery();
                while (rs.next()){
                    int platformId = rs.getInt("platform_dimension_id");
                    int browserId = rs.getInt("browser_dimension_id");
                    int totalUser = rs.getInt("new_members");
                    String k = platformId+"_"+browserId;
                    if(map.containsKey(k)){
                        totalUser +=  map.get(k);
                    }
                    map.put(k,totalUser);
                }
            }
            //更新用户模块新增总用户数据
            ps = conn.prepareStatement("insert into `stats_device_browser`(`date_dimension_id`,`platform_dimension_id`,`browser_dimension_id`,`total_members`) values(?,?,?,?) on duplicate key update `total_members` = ?");
            //循环map
            for (Map.Entry<String,Integer> entry:map.entrySet()) {
                ps.setInt(1,nowDateDimensionId);
                String[] ks = entry.getKey().split("_");
                ps.setInt(2,Integer.parseInt(ks[0]));
                ps.setInt(3,Integer.parseInt(ks[1]));
                ps.setInt(4,entry.getValue());
                ps.setInt(5,entry.getValue());
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtil.close(conn,ps,rs);
        }
    }

    /**
     *
     * @param conn
     * @param dateDimension
     */
    private int getDateDimensionId(Connection conn, DateDimension dateDimension) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        int i = 0;
        int dimensionId = 0;
        try {
            ps = conn.prepareStatement("select `id` from `dimension_date` where `year` = ? and `season` = ? and `month` = ? and `week` = ? and `day` = ? and `type` = ? and `calendar` = ?");
            ps.setInt(++i,dateDimension.getYear());
            ps.setInt(++i,dateDimension.getSeason());
            ps.setInt(++i,dateDimension.getMonth());
            ps.setInt(++i,dateDimension.getWeek());
            ps.setInt(++i,dateDimension.getDay());
            ps.setString(++i,dateDimension.getType());
            ps.setDate(++i,new Date(dateDimension.getCalendar().getTime()));
            rs = ps.executeQuery();
            if(rs.next()){
                dimensionId = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtil.close(null,ps,rs);
        }
        return dimensionId;
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
                EventLogConstant.LOG_COLUMN_NAME_MEMBER_ID,
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