package com.wsk.bigdata.mapreduce.mapper;

import com.wsk.bigdata.pojo.Info;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * 文件间的Mapjoin
 */
public class FileMapJoinMapper extends Mapper<LongWritable, Text, Info, NullWritable> {

	/**
	 * 产品信息信息集合，key=产品ID，value=产品信息
	 */
	private Map<String, Info> infos = new HashMap<>();


	/**
	 * 执行Map方法前会调用一次setup方法，我们可以用于
	 * 初始化读取产品信息加到到内存中
	 *
	 */
	@Override

	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println("--------MAP初始化：加载产品信息数据到内存------");
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(System.getProperty("product.info.dir"))));
		String line;

		while (StringUtils.isNotEmpty(line = br.readLine())) {
			String[] fields = line.split(",");
			if (fields != null && fields.length == 4) {
				Info info = new Info(fields[0], fields[1], Float.parseFloat(fields[2]), fields[3], "", "", "", "1");
				infos.put(fields[0], info);
			}
		}
		br.close();
		System.out.println("--------MAP初始化：共加载了" + infos.size() + "条产品信息数据------");

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split(",");
		if (fields != null && fields.length == 4) {
			String pid = fields[1];
			Info produceInfo = infos.get(pid);
			if (produceInfo == null) {
				return;
			}
			Info info = new Info(produceInfo.getpId(), produceInfo.getpName(), produceInfo.getPrice(), produceInfo.getProduceArea()
					, fields[0], fields[2], fields[3], null);
			context.write(info, NullWritable.get());

		}

	}

}
