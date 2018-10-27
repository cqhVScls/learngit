import edu.qianfeng.anlastic.model.dim.base.EventDimension;
import edu.qianfeng.anlastic.service.IDimensionConvertor;
import edu.qianfeng.anlastic.service.impl.IDimensionConvertorImpl;

import java.io.IOException;

/**
 * Created by lyd on 2018/6/6.
 */
public class LocationTest {
    public static void main(String[] args) {
        IDimensionConvertor convertor = new IDimensionConvertorImpl();
        //LocationDiemension local = new LocationDiemension("中国","河南","郑州");
        EventDimension event = new EventDimension("aa", "cc");
        try {
            System.out.println(convertor.getDimensionIdByValue(event));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
