package com.wsk.bigdata.utils;


import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

/**
 * @Description: CDN 点击日志生产工具类
 * @Author: skwang2@iflytek.com
 * @CreateDate: 2019/4/1 17:02
 * @Version: 1.0
 **/
public class CDNLogProducer {

	private static Random random = new Random();

	public static void main(String[] args) {

		if (args.length != 2) {
			System.err.println(" please input 2 params: num outTextPath ");
			System.exit(0);
		}
		int num = Integer.parseInt(args[0]);

		System.out.println("++++++++++++++++++ begining produece logs ++++++++++++++++++++++++");
		long t1 = System.currentTimeMillis();
		File file = new File(args[1]);
		File absoluteFile = file.getAbsoluteFile();
		File parentFile = absoluteFile.getParentFile();
		if (!parentFile.exists()) {
			parentFile.mkdirs();
		}
		if (absoluteFile.exists()) {
			absoluteFile.delete();
		}

		FileOutputStream ops = null;
		try {
			absoluteFile.createNewFile();
			ops = new FileOutputStream(file);
			for (int i = 0; i < num; i++) {
				String[] splits = new String[8];
				String cdn = "baidu";
				String region = "CN";
				String level = "3";
				String timeStr = getRandomTime();
				String ip = getRandomIp();
				String domain = getRandomDomain();
				String url = getRandomURL(domain);
				String traffic = getRandomTra();

				splits[0] = cdn;
				splits[1] = region;
				splits[2] = level;
				splits[3] = timeStr;
				splits[4] = ip;
				splits[5] = domain;
				splits[6] = url;
				if (i < num - 1) {
					splits[7] = traffic + "\n";
				} else {
					splits[7] = traffic;
				}
				String log = StringUtils.join(splits, ",");
				ops.write(log.getBytes());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (ops != null) {
					ops.flush();
					ops.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		int time = (int) ((System.currentTimeMillis() - t1) / 1000);
		System.out.println("++++++++++++++++++ ending produece logs process,It takes " + time + " seconds. " +
				"++++++++++++++++++++++++");

	}

	private static String getRandomDomain() {
		return "v2.go2yd." + getRandom(1, 10) + "com";
	}

	private static String getRandomTra() {
		return "" + getRandom(10000, 100000);
	}

	private static String getRandomURL(String domain) {
		return "http://" + domain + "/resource_" + getRandom(1, 100) + ".mp4";
	}

	private static String getRandomIp() {
		return "223.104." + getRandom(1, 256) + "." + getRandom(1, 256);
	}

	private static String getRandomTime() {
		StringBuilder sb = new StringBuilder("2019");
//		int month = getRandom(1, 13);
		int month = 5;
		if (month < 10) {
			sb.append("0");
		}
		sb.append(month);
//		int day = getRandom(1, 29);
		int day = 6;
		if (day < 10) {
			sb.append("0");
		}
		sb.append(day);
		int hour = getRandom(1, 4);
		if (hour < 10) {
			sb.append("0");
		}
		sb.append(hour);
		sb.append("01");
		sb.append("01");
		return sb.toString();
	}

	private static int getRandom(int start, int end) {
		return random.nextInt(end - start) + start;
	}
}