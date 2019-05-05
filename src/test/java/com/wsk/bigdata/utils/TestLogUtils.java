package com.wsk.bigdata.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 欢迎来到若泽数据(www.ruozedata.com)
 * 讲师：若泽(PK哥)  QQ：1990218038
 */
public class TestLogUtils {

    private LogUtils utils ;

    @Test
    public void testLogParse() {
        String log = "baidu\tCN\tA\tE\t[17/Jul/2018:17:07:50 +0800]\t2\t223.104.18.110\t-\t112.29.213.35:80\t0\tv2.go2yd.com\tGET\thttp://v1.go2yd.com/user_upload/1531633977627104fdecdc68fe7a2c4b96b2226fd3f4c.mp4_bd.mp4\tHTTP/1.1\t-\tbytes 13869056-13885439/25136186\tTCP_HIT/206\t112.29.213.35\tvideo/mp4\t17168\t16384\t-:0\t0\t0\t-\t-\t-\t11451601\t-\t\"JSP3/2.0.14\"\t\"-\"\t\"-\"\t\"-\"\thttp\t-\t2\tv1.go2yd.com\t0.002\t25136186\t16384\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t1531818470104-11451601-112.29.213.66#2705261172\t644514568\n";
        String result = utils.parse(log);
        System.out.println(result);
    }


    @Before
    public void setUp(){
        utils = new LogUtils();
    }

    @After
    public void tearDown(){
        utils = null;
    }




}
