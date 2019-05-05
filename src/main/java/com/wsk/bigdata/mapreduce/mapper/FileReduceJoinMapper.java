package com.wsk.bigdata.mapreduce.mapper;

import com.wsk.bigdata.pojo.Info;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class FileReduceJoinMapper extends Mapper<LongWritable, Text, Text, Info> {


	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] fields = line.split(",");
		String pid = "";
		Info info = null;
		// 通过文件名判断是哪种数据
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		String name = inputSplit.getPath().getName();
		if (name.startsWith("product")) {
			pid=fields[0];
			info = new Info(pid,fields[1],Float.parseFloat(fields[2]),fields[3],"","","","1");
		} else {
			pid=fields[1];
			info = new Info(pid,"",0,"",fields[0],fields[2],fields[3],"0");
		}
		if(info==null){
			return;
		}

		k.set(pid);
		System.out.println("map 输出"+info.toString());
		context.write(k, info);
	}
}
