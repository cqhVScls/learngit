package edu.qianfeng.util;

import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by lyd on 2018/6/5.
 *
 * 操作会员的工具类，判断会员id是否为新增会员
CREATE TABLE `member_info` (
 `member_id` varchar(32) NOT NULL DEFAULT '' COMMENT '会员id，是一个最多32位的字母数字字符串',
 `last_visit_date` date DEFAULT NULL COMMENT '最后访问日期',
 `member_id_server_date` BIGINT(11) DEFAULT NULL COMMENT '第一次出现memberid的时间',
 `created` date DEFAULT NULL,
 PRIMARY KEY (`member_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 */
public class MemberUtil {
    //key=memberId   value=true/false
    public static Map<String,Boolean> cache = new LinkedHashMap<String,Boolean>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return this.size() > 1000;//大于1000个memberId将会移除
        }
    };


    /**
     * 判断memberId是否为一个新增会员
     * @param memberId
     * @param conn
     * @return  true:新用户  false:老用户或者无效用户(memberId为空)
     */
    public static boolean isNewMember(String memberId, Connection conn){

        PreparedStatement ps = null;
        ResultSet rs = null;

        Boolean isNewMemberId = null;
        if(StringUtils.isNotEmpty(memberId.trim())){
            //memberId不等于null
            //从缓存中查找是否有该memberId
            isNewMemberId = cache.get(memberId);
            if(isNewMemberId == null){  //证明缓存中没有该memberid，所以需要去数据库查询
                try {
                    ps = conn.prepareStatement("SELECT `member_id` from `member_info` where `member_id` = ?");
                    ps.setString(1,memberId);
                    rs = ps.executeQuery();
                    if(rs.next()){
                        isNewMemberId = Boolean.valueOf(false);
                    } else {
                        isNewMemberId = Boolean.valueOf(true);
                    }
                    cache.put(memberId,isNewMemberId); //添加到缓存中
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    JDBCUtil.close(conn,ps,rs);
                }
            }
        }
        //如果等于null,证明该会员id有问题，所以判断为非新用户
        return isNewMemberId == null ? false : isNewMemberId.booleanValue();
    }

    /**
     * 按照日期删除当天的所有会员
     * @param date
     * @param conn
     */
    public static void deleteMemberInfoByDate(String date,Connection conn){
        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement("DELETE FROM `member_info` where `created` = ?");
            ps.setString(1,date);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtil.close(conn,ps,null);
        }
    }
}
