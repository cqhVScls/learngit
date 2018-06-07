package edu.qianfeng.common;

/**
 * Created by lyd on 2018/5/31.
 * kpi的枚举
 */
public enum  KpiType {
    NEW_INSTALL_USER("new_install_user"),
    BROWSER_NEW_INSTALL_USER("browser_new_install_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    ACTIVE_MEMBER("active_member"),
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member"),
    MEMBER_INFO("member_info"),
    SESSION("session"),
    BROWSER_SESSION("browser_session"),
    HOURLY_ACTIVE_USER("hourly_active_user"),
    HOURLY_SESSIOIN("hourly_session"),
    PAGE_VIEW("page_view"),
    LOCATION("location"),
    ;

    public String kpiName;

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    /**
     * 根据kpi的名字获取kpi的枚举
     * @param kpiName
     * @return
     */
    public static KpiType valueOfKpiName(String kpiName){
        for (KpiType kpiType: values()) {
            if(kpiName.equals(kpiType.kpiName)){
                return kpiType;
            }
        }
        return null;
    }

}
