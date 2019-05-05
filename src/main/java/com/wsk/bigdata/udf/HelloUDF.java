package com.wsk.bigdata.udf;

        import org.apache.hadoop.hive.ql.exec.UDF;

public class HelloUDF extends UDF {

    public String evaluate(String value) {
        return "hello:" + value;
    }

    public static void main(String[] args) {
        HelloUDF helloUDF = new HelloUDF();
        String value = helloUDF.evaluate("wsk");
        System.out.println(value);
    }
}
