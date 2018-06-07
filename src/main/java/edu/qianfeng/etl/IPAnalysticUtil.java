package edu.qianfeng.etl;

import com.alibaba.fastjson.JSONObject;
import edu.qianfeng.common.GlobalConstants;
import edu.qianfeng.etl.ip.IPSeeker;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;


/**
 * Created by lyd on 2018/5/29.
 * 解析ip的工具类
 * :如果IP是null，则返回null。
 * 如果省份未知：contury unkown unknown
 */
public class IPAnalysticUtil extends IPSeeker {
    RegionInfo info = new RegionInfo();

    /**
     * 根据ip获取区域对象
     *
     * @param ip 192.168.216.1^A1527558228.293^A192.168.216.111^A/demo.html
     * @return
     */
    public RegionInfo getRegionByIp(String ip) {
        //如果ip为空则直接返回
        if (ip == null || StringUtils.isEmpty(ip.trim())) {
            return info;
        }
        //正常解析ip
        try {
            //通过父类获取国家
            String country = super.getCountry(ip);
            if (country != null && country.equals("局域网")) {
                //设置国家、省、市
                info.setCountry("中国");
                info.setProvince("北京");
                info.setCity("昌平区");
            } else if (!country.trim().isEmpty()) {
                //定位省的位置
                int index = country.indexOf("省");
                info.setCountry("中国");
                //如果省的位置》0证明有省
                if (index > 0) {
                    //设置省份
                    info.setProvince(country.substring(0, index + 1));
                    //判断省后面是否有市
                    int index2 = country.indexOf("市");
                    if (index2 > 0) {
                        //设置市
                        info.setCity(country.substring(index + 1, index2 + 1));
                    }
                } else {
                    //如果没有省，则有可能是直辖市或者是特别行政qu
                    String flag = country.substring(0, 2);
                    switch (flag) {
                        case "内蒙":
                            info.setProvince(flag + "古");
                            country = country.substring(3);
                            index = country.indexOf("市");
                            if (index > 0) {
                                //设置市
                                info.setCity(country.substring(0, index + 1));
                            }
                            break;
                        case "新疆":
                        case "广西":
                        case "宁夏":
                        case "西藏":
                            info.setProvince(flag);
                            country = country.substring(2);
                            index = country.indexOf("市");
                            if (index > 0) {
                                //设置市
                                info.setCity(country.substring(0, index + 1));
                            }
                            break;
                        case "北京":
                        case "上海":
                        case "天津":
                        case "重庆":
                            info.setProvince(flag+"市");
                            country = country.substring(3);
                            index = country.indexOf("区");
                            if (index > 0) {
                                char ch = country.charAt(index - 1);
                                if (ch != '校' || ch != '军' || ch != '小' || ch != '灯' || ch != '误') {
                                    //设置市
                                    info.setCity(country.substring(0, index+1));
                                }
                            }
                            index = country.indexOf("县");
                            if (index > 0) {
                                //设置县
                                info.setCity(country.substring(0, index + 1));
                            }
                            break;
                        case "香港":
                        case "澳门":
                            info.setProvince(flag + "特别行政区");
                            break;
                        default:
                            break;
                    }
                }
            }
            } catch(Exception e){
                e.printStackTrace();
            }
            return info;
        }

    /**
     * @param url :http://ip.taobao.com/service/getIpInfo.php?ip=171.120.0.1
     * @param charset "utf-8"
     * @return
     * @throws Exception
     */
    public static RegionInfo parserIp1(String url, String charset) throws Exception {
        RegionInfo info = new RegionInfo();
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(url);

        if (null == url || !url.startsWith("http")) {
            throw new Exception("请求地址格式不对");
        }
        // 设置请求的编码方式
        if (null != charset) {
            method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=" + charset);
        } else {
            method.addRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=" + "utf-8");
        }
        int statusCode = client.executeMethod(method);

        if (statusCode != HttpStatus.SC_OK) {// 打印服务器返回的状态
            System.out.println("Method failed: " + method.getStatusLine());
        }
        // 返回响应消息
        byte[] responseBody = method.getResponseBodyAsString().getBytes(method.getResponseCharSet());
        // 在返回响应消息使用编码(utf-8或gb2312)
        String response = new String(responseBody, "utf-8");

        JSONObject jo = JSONObject.parseObject(response);
        JSONObject j = (JSONObject)jo.get("data");

        // 释放连接
        method.releaseConnection();
        //设置省市
        info.setCountry(j.get("country").toString());
        info.setProvince(j.get("region").toString());
        info.setCity(j.get("city").toString()+"市");
        return info;
    }


    /**
         * 用于封装国家省市的类
         */
        public static class RegionInfo {
            public static final String DEFAULT_VALUE = GlobalConstants.DEFAULT_VALUE; //默认为未知
            public static String country = DEFAULT_VALUE;//国家
            public static String province = DEFAULT_VALUE;//省
            public static String city = DEFAULT_VALUE;//市

            public static void setCountry(String country) {
                RegionInfo.country = country;
            }

            public static void setProvince(String province) {
                RegionInfo.province = province;
            }

            public static void setCity(String city) {
                RegionInfo.city = city;
            }

            public String getCountry() {
                return country;
            }
            public String getProvince() {
                return province;
            }

            public String getCity() {
                return city;
            }


            @Override
            public String toString() {
                return "RegionInfo{" + country + "\t" + province + "\t" + city + "}";
            }
        }
}
