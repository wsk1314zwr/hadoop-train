package com.wsk.bigdata.mapreduce.driver;

import com.wsk.bigdata.mapreduce.mapper.LogETLMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 欢迎来到若泽数据(www.ruozedata.com)
 * 讲师：若泽(PK哥)  QQ：1990218038
 */
public class LogETLDriver {

    public static void main(String[] args) throws Exception{
        if(args.length != 2) {
            System.err.println("please input 2 params: input output");
            System.exit(0);
        }

        String input = args[0];
        String output = args[1];  //output/d=20180717

//        System.setProperty("hadoop.home.dir", "D:\\appanzhuang\\cdh\\hadoop-2.6.0-cdh5.7.0");


        Configuration configuration = new Configuration();

        // 写代码：死去活来法
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(output);
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);
        job.setJarByClass(LogETLDriver.class);
        job.setMapperClass(LogETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
