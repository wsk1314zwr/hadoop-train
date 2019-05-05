package com.wsk.bigdata.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Random;

public class RemovePre extends UDF {

    public String evaluate(String value) {
        String[] strings = StringUtils.split(value, "_");
        return strings[1];
    }

    public static void main(String[] args) {
        RemovePre helloUDF = new RemovePre();
        for (int i=10;i<20;i++){
            Random random = new Random();
            String value = helloUDF.evaluate(random.nextInt(10)+"_wsk");
            System.out.println(value);
        }

    }
}
