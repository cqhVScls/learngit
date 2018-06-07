import edu.qianfeng.etl.UserAgentUtil;

/**
 * Created by lyd on 2018/5/30.
 */
public class UserAegntTest {
    public static void main(String[] args) {
        System.out.println(new UserAgentUtil().getUserAgentInfoByUA("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)"));
    }
}
