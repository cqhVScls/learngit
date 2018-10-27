package edu.qianfeng.etl;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by lyd on 2018/5/30.
 * 清洗userAgent
 */
public class UserAgentUtil {
    public static final Logger logger = Logger.getLogger(UserAgentUtil.class);
//Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F58.0.3029.110%20Safari%2F537.36%20SE%202.X%20MetaSr%201.0

    //获取一个usaparser的对象
    public static UASparser uas = null;

    //该静态代码块专用于uasparser的初始化
    static {
        try {
            uas = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.warn("获取usaparser对象异常", e);
        }
    }

    /**
     * 根据传递过来userAgent解析出其浏览器和操作系统信息
     *
     * @param userAgent
     * @return
     */
    public UserAgentInfo getUserAgentInfoByUA(String userAgent) {
        UserAgentInfo info = null;
        if (StringUtils.isNotEmpty(userAgent.trim())) {
            //解析
            cz.mallat.uasparser.UserAgentInfo oinfo = null;
            //使用usaparser解析出来包自带的useragentInfo
            try {
                oinfo = uas.parse(userAgent);
                //将oinfo中的属性设置自己的userAgetnInfo中
                if (oinfo != null) {
                    info = new UserAgentInfo();
                    info.setBrowserName(oinfo.getUaFamily());
                    info.setBrowserVersion(oinfo.getBrowserVersionInfo());
                    info.setOsName(oinfo.getOsFamily());
                    info.setOsVersion(oinfo.getOsName()); //版本
                }
            } catch (IOException e) {
                logger.warn("解析userAgent失败", e);
            }
        }
        return info;
    }

    /**
     * 用于封装userAgent对象
     */
    public static class UserAgentInfo {
        public static String browserName;
        public static String browserVersion;
        public static String osName;
        public static String osVersion;

        public static String getBrowserName() {
            return browserName;
        }

        public static void setBrowserName(String browserName) {
            UserAgentInfo.browserName = browserName;
        }

        public static String getBrowserVersion() {
            return browserVersion;
        }

        public static void setBrowserVersion(String browserVersion) {
            UserAgentInfo.browserVersion = browserVersion;
        }

        public static String getOsName() {
            return osName;
        }

        public static void setOsName(String osName) {
            UserAgentInfo.osName = osName;
        }

        public static String getOsVersion() {
            return osVersion;
        }

        public static void setOsVersion(String osVersion) {
            UserAgentInfo.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return browserName + ":\t" + browserVersion + ":\t" + osName + ":\t" + osVersion;
        }

    }
}
