package edu.qianfeng.etl.tohbase;

import edu.qianfeng.common.EventLogConstant;
import edu.qianfeng.etl.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Created by lyd on 2018/5/30.
 * 直接使用mapper将解析后的数据存储到hbase中
 */
public class LogDataToHbaseMapper extends Mapper<Object,Text,NullWritable,Put> {
    public static final Logger logger = Logger.getLogger(LogDataToHbaseMapper.class);
    //定义输入的行数、过滤数、输出数r
    public static int inputRecords,outputRecords,filterRecords = 0;
    //获取列簇
    private byte[] columnFamily = Bytes.toBytes(EventLogConstant.LOG_FAMILY_NAME);

    public CRC32 crc32 = new CRC32();
    /**
     * map方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       inputRecords ++;
        logger.info("解析日志的数据为:"+value.toString());
        Map<String,String> info = LogUtil.handleLog(value.toString());
        if(info == null){
            filterRecords ++;
            return;
        }
        //获取事件名称
        String eventName = info.get(EventLogConstant.LOG_COLUMN_NAME_EVENT_NAME);
        EventLogConstant.EventEnum event = EventLogConstant.EventEnum.valueOfAlias(eventName);
        switch (event) {
            case LAUNCH:
            case PAGEVIEW:
            case CHARGE_REQUEST:
            case CHARGE_SUCCESS:
            case CHARGE_REFUND:
            case EVENT:
                handLogToHbase(info,context,eventName);
                break;
            default:
                filterRecords ++;
                logger.warn("该事件暂时不支持解析，事件名称为 :"+event.alias);
                break;
        }
    }

    /**
     * 用于将info中的数据存储hbase中，使用serverTime_crc32(uuid,umid,eventname)
     * @param info
     * @param context
     */
    private void handLogToHbase(Map<String, String> info, Context context,String eventName) {
        String serverTime = info.get(EventLogConstant.LOG_COLUMN_NAME_SERVER_TIME);
        String uuid = info.get(EventLogConstant.LOG_COLUMN_NAME_UUID);
        String umid = info.get(EventLogConstant.LOG_COLUMN_NAME_MEMBER_ID);
        //判断serverTime是否为空
        if(StringUtils.isNotBlank(serverTime)){
            //生成row-key
            String rowkey = gengerateRowKey(serverTime,umid,uuid,eventName);
            //获取hbase的put对象
            Put put = new Put(rowkey.getBytes());
            //循环info
            for (Map.Entry<String,String> entry:info.entrySet()) {
                if(StringUtils.isNotEmpty(entry.getKey())){
                    put.add(columnFamily,Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getValue()));
                }
            }
            //输出
            try {
                outputRecords ++;
                context.write(NullWritable.get(),put);
            } catch (IOException e) {
                logger.warn("tohbase输出异常1",e);
            } catch (InterruptedException e) {
                logger.warn("tohbase输出中段异常1",e);
            }
        } else {
            filterRecords ++;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("输入数据："+inputRecords+" 过滤数据:"+filterRecords+" 输出数据:"+outputRecords);
    }

    /**
     * 使用crc32来生成rowkey
     * @param serverTime
     * @param umid
     * @param uuid
     * @param eventName
     * @return
     */
    private String gengerateRowKey(String serverTime, String umid, String uuid, String eventName) {
        StringBuffer sb = new StringBuffer();
        sb.append(serverTime+"_");
        //对crc32进行重置值
        crc32.reset();
        if(StringUtils.isNotEmpty(uuid)){
            crc32.update(uuid.getBytes());
        }
        if(StringUtils.isNotEmpty(umid)){
            crc32.update(umid.getBytes());
        }
        this.crc32.update(eventName.getBytes());
        //获取crc32的最终的值
        sb.append(this.crc32.getValue()%1000000000l);
        return sb.toString();
    }
}
