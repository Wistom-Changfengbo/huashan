import com.wangjia.hbase.ExHbase;
import com.wangjia.utils.JavaUtils;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by Administrator on 2017/5/5.
 */
public class Test {

    static final class A {
        String id = "";
        int cc = 0;
        boolean is = true;
        float mm = 1.34214f;

        public A() {
            id = UUID.randomUUID().toString() + UUID.randomUUID().toString() + UUID.randomUUID().toString();
        }
    }


    public static void main(String[] args) throws IOException {
        System.out.println(JavaUtils.ip2Long("117.174.249.18"));
        System.out.println(JavaUtils.ip2HexString("218.67.240.36"));
        System.out.println(JavaUtils.ip2HexString("0.174.249.18"));
        System.out.println(Long.MAX_VALUE);
        System.out.println(Long.toHexString(Long.MAX_VALUE));


        System.out.println(ExHbase.getVisitKey("1110", "aaaaaaaa", 10));
        System.out.println(ExHbase.getVisitKey("1110", "aaaaaaaa", new Date().getTime()));

        System.out.println(ExHbase.getVisitDesKey("1110", "aaaaaaaa", 10));
        System.out.println(ExHbase.getVisitDesKey("1110", "aaaaaaaa", new Date().getTime()));

        long oldt = System.currentTimeMillis();
        long freem = Runtime.getRuntime().freeMemory();

        Map<String, A> map = new HashMap<>(16384);
        for (int i = 0; i < 10000; i++) {
            map.put(UUID.randomUUID().toString(), new A());
        }

        System.out.println(System.currentTimeMillis() - oldt);
        System.out.println((Runtime.getRuntime().freeMemory() - freem) / 1024.0 / 1024.0);

        System.in.read(new byte[100]);
    }
}
