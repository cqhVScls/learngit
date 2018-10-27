package edu.qianfeng.etl;

import edu.qianfeng.common.EventLogConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lyd on 2018/5/30.
 * 日志解析 ：192.168.216.1^A1527560563.475^A192.168.216.111^A/demo.html?facevalue=100&high=170&sex=2
 */
public class LogUtil {
    public static final Logger logger = Logger.getLogger(LogUtil.class);

    /**
     * 解析日志
     *
     * @param log ：192.168.216.1^A1527560563.475^A192.168.216.111^A/demo.html?facevalue=100&high=170&sex=2
     * @return map
     */
    public static Map<String, String> handleLog(String log) {
        Map<String, String> map = new HashMap<String, String>();
        if (log != null && StringUtils.isNotEmpty(log.trim())) {
            //拆分
            String[] fields = log.split(EventLogConstant.LOG_COLUMN_NAME_SEPARTOR);
            //判断长度是否为4
            if (fields.length == 4) {
                //获取ip
                map.put(EventLogConstant.LOG_COLUMN_NAME_IP, fields[0]);
                map.put(EventLogConstant.LOG_COLUMN_NAME_SERVER_TIME, fields[1].replaceAll("\\.", ""));
                //判断url中是否存在？
                int index = fields[3].indexOf("?");
                if (index > 0) {
                    //到这儿，代表有参数
                    String params = fields[3].substring(index + 1);
                    //处理参数
                    handleParams(params, map);
                    //处理ip
                    handleIp(map);
                    //处理userAgent
                    handleUserAgent(map);
                }
            }
        }
        return map;
    }

    /**
     * 处理userAgent
     *
     * @param map
     */
    private static void handleUserAgent(Map<String, String> map) {
        if (map != null && map.containsKey(EventLogConstant.LOG_COLUMN_NAME_USER_AGENT)) {
            String userAgent = map.get(EventLogConstant.LOG_COLUMN_NAME_USER_AGENT);
            UserAgentUtil.UserAgentInfo info = new UserAgentUtil().getUserAgentInfoByUA(userAgent);
            //将info中的值一个一个的put进去
            map.put(EventLogConstant.LOG_COLUMN_NAME_BROWSER_NAME, info.getBrowserName());
            map.put(EventLogConstant.LOG_COLUMN_NAME_BROWSER_VERSION, info.getBrowserVersion());
            map.put(EventLogConstant.LOG_COLUMN_NAME_OS_NAME, info.getOsName());
            map.put(EventLogConstant.LOG_COLUMN_NAME_OS_VERSION, info.getOsVersion());
        }
    }

    /**
     * 处理ip的方法
     *
     * @param map
     */
    private static void handleIp(Map<String, String> map) {
        if (map != null && map.containsKey(EventLogConstant.LOG_COLUMN_NAME_IP)) {
            String ip = map.get(EventLogConstant.LOG_COLUMN_NAME_IP);
            IPAnalysticUtil.RegionInfo info = new IPAnalysticUtil().getRegionByIp(ip);
            //将info中的值一个一个的put进去
            map.put(EventLogConstant.LOG_COLUMN_NAME_COUNTRY, info.getCountry());
            map.put(EventLogConstant.LOG_COLUMN_NAME_PROVINCE, info.getProvince());
            map.put(EventLogConstant.LOG_COLUMN_NAME_CITY, info.getCity());
        }
    }

    /**
     * 将参数列表中的参数存储 到map中
     *
     * @param params
     * @param map
     */
    private static void handleParams(String params, Map<String, String> map) {
        if (params != null && map != null) {
            String[] fields = params.split("&");
            for (String field : fields) {
                String[] kvs = field.split("=");
                //facevalue=100&high=&sex=
                if (!kvs[0].trim().equals("")) {
                    try {
                        map.put(kvs[0], URLDecoder.decode(kvs[1], "utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        logger.warn("值的解码异常", e);
                    }
                } else {
                    logger.info("key is not exists:" + kvs[0]);
                    continue;
                }
            }
        }
    }
}
