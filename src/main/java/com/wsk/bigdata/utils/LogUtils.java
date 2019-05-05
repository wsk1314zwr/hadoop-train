package com.wsk.bigdata.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * 欢迎来到若泽数据(www.ruozedata.com)
 * 讲师：若泽(PK哥)  QQ：1990218038
 */
public class LogUtils {
    DateFormat sourceFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    DateFormat targetFormat = new SimpleDateFormat("yyyyMMddHHmmss");


    /**
     * 日志文件解析，对内容进行字段的处理
     * 按\t分割
     */
    public String parse(String log) {
        String result = "";
        try {
            String[] splits = log.split("\t");
            String cdn = splits[0];
            String region = splits[1];
            String level = splits[3];
            String timeStr = splits[4];
            String time = timeStr.substring(1,timeStr.length()-7);
            time = targetFormat.format(sourceFormat.parse(time));
            String ip = splits[6];
            String domain = splits[10];
            String url = splits[12];
            String traffic = splits[20];

//            System.out.println(cdn);
//            System.out.println(region);
//            System.out.println(level);
//            System.out.println(time);
//            System.out.println(ip);
//            System.out.println(domain);
//            System.out.println(url);
//            System.out.println(traffic);

            StringBuilder builder = new StringBuilder("");
            builder.append(cdn).append("\t")
                    .append(region).append("\t")
                    .append(level).append("\t")
                    .append(time).append("\t")
                    .append(ip).append("\t")
                    .append(domain).append("\t")
                    .append(url).append("\t")
                    .append(traffic);

            result = builder.toString();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }
}
