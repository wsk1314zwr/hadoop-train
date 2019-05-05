package com.wsk.bigdata.mapreduce.mapper;

import com.wsk.bigdata.utils.LogUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 欢迎来到若泽数据(www.ruozedata.com)
 * 讲师：若泽(PK哥)  QQ：1990218038
 */
public class LogETLMapper extends Mapper<LongWritable,Text,NullWritable,Text>{

    /**
     * 通过mapreduce框架的map方式进行数据清洗
     * 进来一条数据就按照我们的解析规则清洗完以后输出
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int length = value.toString().split("\t").length;
        if(length == 72) {
            LogUtils utils = new LogUtils();
            String result = utils.parse(value.toString());
            if(StringUtils.isNotBlank(result)) {
                context.write(NullWritable.get(), new Text(result));
            }
        }
    }
}
