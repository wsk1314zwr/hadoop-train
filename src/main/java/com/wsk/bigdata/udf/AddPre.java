package com.wsk.bigdata.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Random;

public class AddPre extends UDF {

    public Random random = new Random();
    public String evaluate(String value) {
        int randonNum = random.nextInt(10);
        return randonNum+"_" + value;
    }

    public static void main(String[] args) {
        AddPre helloUDF = new AddPre();
        for (int i=0;i<10;i++){
            String value = helloUDF.evaluate("wsk");
            System.out.println(value);
        }
    }
}
