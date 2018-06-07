import edu.qianfeng.etl.LogUtil;

import java.util.Map;

/**
 * Created by lyd on 2018/5/30.
 */
public class LogTest {
    public static void main(String[] args) {
      Map<String,String> map = LogUtil.handleLog("192.168.216.1^A1527561145.628^A192.168.216.111^A/demo.html?en=e_e&ca=event%E7%9A%84category%E5%90%8D%E7%A7%B0&ac=event%E7%9A%84action%E5%90%8D%E7%A7%B0&ver=1&pl=website&sdk=js&u_ud=D9122AFE-9FF6-4F6E-BE7D-47CB7FE83B83&u_sd=9BA55609-4175-4C9A-9525-890F4CC64F13&c_time=1527561145687&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F58.0.3029.110%20Safari%2F537.36%20SE%202.X%20MetaSr%201.0&b_rst=1600*900");
        System.out.println(map.toString());
        for (Map.Entry<String,String> entry:map.entrySet()) {
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
    }
}
