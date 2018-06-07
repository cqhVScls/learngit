import edu.qianfeng.etl.IPAnalysticUtil;
import edu.qianfeng.etl.ip.IPSeeker;

import java.util.List;

/**
 * Created by lyd on 2018/5/29.
 */
public class IpTest {

    public static void main(String[] args) {
        IPSeeker ipSeeker = IPSeeker.getInstance();
        System.out.println(IPSeeker.class);
//        System.out.println(ipSeeker.getCountry("222.183.213.40"));
//        System.out.println(new IPAnalysticUtil().getRegionByIp("203.198.23.69"));

        List<String> ips = IPSeeker.getInstance().getAllIp();
        for (String ip: ips) {
//            System.out.println("ip+"+ip+ " "+new IPAnalysticUtil().getRegionByIp(ip));
            try {
                System.out.println("ip+"+ip+ " "+new IPAnalysticUtil().parserIp1("http://ip.taobao.com/service/getIpInfo.php?ip="+ip,"utf-8"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
