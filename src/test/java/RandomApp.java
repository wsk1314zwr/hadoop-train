import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 欢迎来到若泽数据(www.ruozedata.com)
 * 讲师：若泽(PK哥)  QQ：1990218038
 */
public class RandomApp {

    // 2 * 2  20  班长连带
    // 11  27         13
    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        for(int i=1; i<=41; i++) {
            list.add(i);
        }

        Random random = new Random();
        System.out.println(list.get(random.nextInt(list.size())));
    }
}
