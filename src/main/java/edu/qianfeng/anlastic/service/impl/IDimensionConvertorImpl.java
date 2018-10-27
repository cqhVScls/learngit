package edu.qianfeng.anlastic.service.impl;

import edu.qianfeng.anlastic.model.dim.base.*;
import edu.qianfeng.anlastic.service.IDimensionConvertor;
import edu.qianfeng.util.JDBCUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by lyd on 2018/6/1.
 * 操作维度表的接口的具体实现
 */
public class IDimensionConvertorImpl implements IDimensionConvertor {
    public static final Logger logger = Logger.getLogger(IDimensionConvertorImpl.class);
    //key：cacheKey  value：存储对应维度Id
    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 100000000;
        }
    };

    /**
     * 操作维度表的接口具体实现方法
     *
     * @param baseDimension
     * @return
     * @throws IOException
     */
    @Override
    public int getDimensionIdByValue(BaseDimension baseDimension) throws IOException {
        String cacheKey = this.buildCacheKey(baseDimension);
        //如果缓存中有对应维度的Id则直接从缓存中返回，不用去操作mysql的数据库
        if (this.cache.containsKey(cacheKey)) {
            return this.cache.get(cacheKey);
        }
        //代码走到这儿，缓存里面还没有该 key-value
        //1、先查询是否有对应维度Id
        //2、如果查询没有，则插入再返回对应的维度Id
        Connection conn = JDBCUtil.getconn();
        //构造查询和插入的sql语句
        String[] sqls = null;
        if (baseDimension instanceof DateDimension) {
            sqls = this.buildDateSqls();
        } else if (baseDimension instanceof BrowserDimension) {
            sqls = this.buildBrowserSqls();
        } else if (baseDimension instanceof PlatformDimension) {
            sqls = this.buildPlatformSqls();
        } else if (baseDimension instanceof KpiDimension) {
            sqls = this.buildKpiSqls();
        } else if (baseDimension instanceof LocationDiemension) {
            sqls = this.buildLocationSqls();
        } else if (baseDimension instanceof EventDimension) {
            sqls = this.buildEventSqls();
        } else if (baseDimension instanceof CurrencyTypeDimension) {
            sqls = buildCurrencySqls(baseDimension);
        } else if (baseDimension instanceof PaymentTypeDimension) {
            sqls = buildPaymentSqls(baseDimension);
        } else {
            throw new RuntimeException("该dimension暂不支持获取对应的sql.dimesion:" + baseDimension.getClass());
        }

        int id = 0;
        //执行对应的sql语句
        synchronized (this) {
            id = this.execute(conn, cacheKey, sqls, baseDimension);
        }
        return id;
    }

    /**
     * 构建缓存key
     *
     * @param baseDimension
     * @return
     */
    private String buildCacheKey(BaseDimension baseDimension) {
        StringBuffer sb = new StringBuffer();
        if (baseDimension instanceof DateDimension) {
            sb.append("date_");
            DateDimension date = (DateDimension) baseDimension;
            sb.append(date.getYear()).append(date.getSeason()).append(date.getMonth())
                    .append(date.getWeek()).append(date.getDay()).append(date.getType());
        } else if (baseDimension instanceof BrowserDimension) {
            sb.append("browser_");
            BrowserDimension browser = (BrowserDimension) baseDimension;
            sb.append(browser.getBrowserName()).append(browser.getBrowserVersion());
        } else if (baseDimension instanceof PlatformDimension) {
            sb.append("platform_");
            PlatformDimension platform = (PlatformDimension) baseDimension;
            sb.append(platform.getPlatformName());
        } else if (baseDimension instanceof KpiDimension) {
            sb.append("kpi_");
            KpiDimension kpi = (KpiDimension) baseDimension;
            sb.append(kpi.getKpiName());
        } else if (baseDimension instanceof LocationDiemension) {
            sb.append("local_");
            LocationDiemension local = (LocationDiemension) baseDimension;
            sb.append(local.getCountry());
            sb.append(local.getProvince());
            sb.append(local.getCity());
        } else if (baseDimension instanceof EventDimension) {
            sb.append("event_");
            EventDimension event = (EventDimension) baseDimension;
            sb.append(event.getCategory());
            sb.append(event.getAction());
        } else if (baseDimension instanceof CurrencyTypeDimension) {
            sb.append("currency_dimension");
            CurrencyTypeDimension currency = (CurrencyTypeDimension) baseDimension;
            sb.append(currency.getCurrencyName());
        } else if (baseDimension instanceof PaymentTypeDimension) {
            sb.append("payment_dimension");
            PaymentTypeDimension payment = (PaymentTypeDimension) baseDimension;
            sb.append(payment.getPaymentType());
        }

        if (sb.length() == 0) {
            throw new RuntimeException("该dimension暂不支持构造cachekey.dimesion:" + baseDimension.getClass());
        }
        return sb.toString();
    }


    /**
     * 创建date维度的查询和插入的sql语句
     *
     * @return
     */
    private String[] buildDateSqls() {
        String querySql = "select `id` from `dimension_date` where `year` = ? and `season` = ? and `month` = ? and `week` = ? and `day` = ? and `type` = ? and `calendar` = ?";
        String insertSql = "insert `dimension_date`(`year`,`season` ,`month` ,`week` ,`day` ,`type` ,`calendar`) values(?,?,?,?,?,?,?)";
        return new String[]{querySql, insertSql};
    }

    private String[] buildKpiSqls() {
        String querySql = "select `id` from `dimension_kpi` where `kpi_name`  = ?";
        String insertSql = "insert `dimension_kpi`(`kpi_name`) values(?)";
        return new String[]{querySql, insertSql};
    }

    private String[] buildPlatformSqls() {
        String querySql = "select `id` from `dimension_platform` where `platform_name`  = ?";
        String insertSql = "insert `dimension_platform`(`platform_name`) values(?)";
        return new String[]{querySql, insertSql};
    }

    private String[] buildBrowserSqls() {
        String querySql = "select `id` from `dimension_browser` where `browser_name`  = ? and `browser_version` = ?";
        String insertSql = "insert `dimension_browser`(`browser_name`,`browser_version`) values(?,?)";
        return new String[]{querySql, insertSql};
    }

    private String[] buildLocationSqls() {
        String querySql = "select `id` from `dimension_location` where `country`  = ? and `province` = ? and `city` = ?";
        String insertSql = "insert `dimension_location`(`country`,`province`,`city`) values(?,?,?)";
        return new String[]{querySql, insertSql};
    }

    private String[] buildEventSqls() {
        String querySql = "select `id` from `dimension_event`  where `category`  = ?  and `action` = ?";
        String insertSql = "insert `dimension_event` (`category`,`action`) values(?,?)";
        return new String[]{querySql, insertSql};

    }


    private String[] buildPaymentSqls(BaseDimension dimension) {
        String selectSql = "select id from dimension_payment_type where payment_type = ? ";
        String insertSql = "insert into dimension_payment_type(payment_type) values(?)";
        return new String[]{selectSql, insertSql};
    }

    private String[] buildCurrencySqls(BaseDimension dimension) {
        String selectSql = "select id from dimension_currency_type where currency_name = ? ";
        String insertSql = "insert into dimension_currency_type(currency_name) values(?)";
        return new String[]{selectSql, insertSql};
    }


    /**
     * 执行
     *
     * @param conn
     * @param cacheKey
     * @param sqls
     * @param baseDimension
     * @return
     */
    private int execute(Connection conn, String cacheKey, String[] sqls, BaseDimension baseDimension) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareCall(sqls[0]);
            //设置参数
            this.setArgs(ps, baseDimension);
            //执行
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            //代码走到这儿，证明维度表中没有对应的维度，所以需要先insert然后再  返回对应的id
            ps = conn.prepareStatement(sqls[1], Statement.RETURN_GENERATED_KEYS);
            //设置参数
            this.setArgs(ps, baseDimension);
            //执行插入
            ps.executeUpdate();
            rs = ps.getGeneratedKeys(); //获取自动生成key
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.warn("sql异常", e);
        } finally {
            JDBCUtil.close(conn, ps, rs);
        }
        throw new RuntimeException("从数据库 获取维度Id失败");
    }

    /**
     * 为各个ps语句设置参数
     *
     * @param ps
     * @param baseDimension
     */
    private void setArgs(PreparedStatement ps, BaseDimension baseDimension) {
        int i = 0;
        try {
            if (baseDimension instanceof DateDimension) {
                DateDimension date = (DateDimension) baseDimension;
                ps.setInt(++i, date.getYear());
                ps.setInt(++i, date.getSeason());
                ps.setInt(++i, date.getMonth());
                ps.setInt(++i, date.getWeek());
                ps.setInt(++i, date.getDay());
                ps.setString(++i, date.getType());
                ps.setDate(++i, new Date(date.getCalendar().getTime()));
            } else if (baseDimension instanceof BrowserDimension) {
                BrowserDimension browser = (BrowserDimension) baseDimension;
                ps.setString(++i, browser.getBrowserName());
                ps.setString(++i, browser.getBrowserVersion());
            } else if (baseDimension instanceof KpiDimension) {
                KpiDimension kpi = (KpiDimension) baseDimension;
                ps.setString(++i, kpi.getKpiName());
            } else if (baseDimension instanceof PlatformDimension) {
                PlatformDimension platform = (PlatformDimension) baseDimension;
                ps.setString(++i, platform.getPlatformName());
            } else if (baseDimension instanceof LocationDiemension) {
                LocationDiemension local = (LocationDiemension) baseDimension;
                ps.setString(++i, local.getCountry());
                ps.setString(++i, local.getProvince());
                ps.setString(++i, local.getCity());
            } else if (baseDimension instanceof EventDimension) {
                EventDimension event = (EventDimension) baseDimension;
                ps.setString(++i, event.getCategory());
                ps.setString(++i, event.getAction());
            } else if (baseDimension instanceof CurrencyTypeDimension) {
                CurrencyTypeDimension currency = (CurrencyTypeDimension) baseDimension;
                ps.setString(++i, currency.getCurrencyName());
            } else if (baseDimension instanceof PaymentTypeDimension) {
                PaymentTypeDimension payment = (PaymentTypeDimension) baseDimension;
                ps.setString(++i, payment.getPaymentType());
            } else {
                throw new RuntimeException("为sql赋值参数运行异常:" + baseDimension.getClass());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}