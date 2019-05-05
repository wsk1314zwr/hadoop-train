package com.wsk.bigdata.mapreduce.driver;

import com.wsk.bigdata.mapreduce.mapper.FileMapJoinMapper;
import com.wsk.bigdata.pojo.Info;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapJoinDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		if(args.length != 3 ) {
			System.err.println("please input 3 params: product_File page_click_file output_mapjoin directory");
			System.exit(0);
		}
		String productInfo = args[0];
		String input = args[1];
		String output = args[2];
		System.setProperty("hadoop.home.dir", "D:\\appanzhuang\\cdh\\hadoop-2.6.0-cdh5.7.0");
		System.setProperty("product.info.dir",productInfo);
		Configuration conf = new Configuration();
		// 写代码：死去活来法
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(output);
		if(!fs.exists(new Path(productInfo))){
			System.err.println("not found File "+productInfo);
			System.exit(0);
		}
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		Job job = Job.getInstance(conf);
		job.setJarByClass(MapJoinDriver.class);
		job.setMapperClass(FileMapJoinMapper.class);
		// 指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Info.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		// map端join的逻辑不需要reduce阶段，设置reducetask数量为0
		job.setNumReduceTasks(0);

		boolean res = job.waitForCompletion(true);
	}
}
