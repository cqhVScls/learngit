package edu.qianfeng.anlastic.hive;

import edu.qianfeng.anlastic.service.IDimensionConvertor;
import edu.qianfeng.anlastic.service.impl.IDimensionConvertorImpl;
import edu.qianfeng.util.JDBCUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 根据维度Id的组合获取总金额
 * Created by lyd on 2018/4/9.
 */
public class TotalAmountUdf extends UDF {

    public IDimensionConvertor converter = null;

    public TotalAmountUdf() {
        converter = new IDimensionConvertorImpl();
    }


    /**
     * 根据日期和flag获取对应的前天的总金额
     *
     * @param date
     * @return
     */
    public int evaluate(int platform, int date, int currency, int payment) {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = JDBCUtil.getconn();
            ps = conn.prepareStatement("select total_revenue_amount from stats_order where platform_dimension_id = ? and date_dimension_id = ? and currency_type_dimension_id = ? and payment_type_dimension_id = ?");
            ps.setInt(1, platform);
            ps.setInt(2, date);
            ps.setInt(3, currency);
            ps.setInt(4, payment);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt("total_revenue_amount");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtil.close(conn, ps, rs);
        }
        throw new RuntimeException("获取昨天的订单总金额失败");
    }


    public static void main(String[] args) {

        System.out.println(new TotalAmountUdf().evaluate(1, 2, 1, 1));
    }
}
