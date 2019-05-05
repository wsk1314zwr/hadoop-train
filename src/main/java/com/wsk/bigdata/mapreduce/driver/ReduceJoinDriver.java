package com.wsk.bigdata.mapreduce.driver;

import com.wsk.bigdata.mapreduce.mapper.FileReduceJoinMapper;
import com.wsk.bigdata.mapreduce.reduce.FileReduceJoinReducer;
import com.wsk.bigdata.pojo.Info;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceJoinDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("please input 2 params: inpt_data output_mapjoin directory");
			System.exit(0);
		}
		String input = args[0];
		String output = args[1];
		System.setProperty("hadoop.home.dir", "D:\\appanzhuang\\cdh\\hadoop-2.6.0-cdh5.7.0");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(output);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}


		Job job = Job.getInstance(conf);
		job.setJarByClass(ReduceJoinDriver.class);
		job.setMapperClass(FileReduceJoinMapper.class);
		job.setReducerClass(FileReduceJoinReducer.class);
		// 指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Info.class);
		//定义Reducer输出数据的kv类型
		job.setOutputKeyClass(Info.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		boolean res = job.waitForCompletion(true);

		if (!res) {
			System.err.println("error：作业执行失败");
//			System.err.println(job.getF);

		}
	}
}
