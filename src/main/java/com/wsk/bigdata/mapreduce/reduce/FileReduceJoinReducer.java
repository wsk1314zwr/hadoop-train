package com.wsk.bigdata.mapreduce.reduce;

import com.wsk.bigdata.pojo.Info;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileReduceJoinReducer extends Reducer<Text, Info, Info, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<Info> values, Context context) throws IOException, InterruptedException {

		Info pInfo = new Info();
		List<Info> clickBeans = new ArrayList<Info>();

		Iterator<Info> iterator = values.iterator();
		while (iterator.hasNext()) {
			Info bean = iterator.next();
			System.out.println("reduce接收 "+bean);
			if ("1".equals(bean.getFlag())) { //产品
				try {
					BeanUtils.copyProperties(pInfo, bean);
				} catch (IllegalAccessException | InvocationTargetException e) {
					e.printStackTrace();
				}
			} else {
				Info clickBean = new Info();
				try {
					BeanUtils.copyProperties(clickBean, bean);
					clickBeans.add(clickBean);
				} catch (IllegalAccessException | InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}

		// 拼接数据获取最终结果
		for (Info bean : clickBeans) {
			bean.setpName(pInfo.getpName());
			bean.setPrice(pInfo.getPrice());
			bean.setProduceArea(pInfo.getProduceArea());
			System.out.println("reduce结果输出："+bean.toString());
			context.write(bean, NullWritable.get());
		}
	}

}
