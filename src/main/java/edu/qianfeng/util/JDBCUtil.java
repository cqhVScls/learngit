package edu.qianfeng.util;


import edu.qianfeng.common.GlobalConstants;
import org.apache.log4j.Logger;

import java.sql.*;

/**
 * Created by lyd on 2018/6/1.
 * 操作数据库连接工具
 */
public class JDBCUtil {
    public static final Logger logger = Logger.getLogger(JDBCUtil.class);

    static {
        try {
            Class.forName(GlobalConstants.DRIVER);
        } catch (ClassNotFoundException e) {
            logger.warn("获取驱动类失败",e);
        }
    }

    /**
     * 获取conn
     * @return
     */
    public static Connection getconn(){
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(GlobalConstants.URL,GlobalConstants.USERNAME,GlobalConstants.PASSWORD);
        } catch (SQLException e) {
            logger.warn("获取连接connection异常",e);
        }
        return conn;
    }

    /**
     * 关闭所有与mysql有关的对象
     * @param conn
     * @param ps
     * @param rs
     */
    public static void close(Connection conn, PreparedStatement ps, ResultSet rs){
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                //do nothing
            }
        }

        if(ps != null){
            try {
                ps.close();
            } catch (SQLException e) {
                //do nothing
            }
        }

        if(rs != null){
            try {
                rs.close();
            } catch (SQLException e) {
                //do nothing
            }
        }
    }
}
