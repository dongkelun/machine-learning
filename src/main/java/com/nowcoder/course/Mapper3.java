package com.nowcoder.course;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper3 extends Mapper<Object, Text, Text, Text> {
	private Text uid = new Text();
	private Text mid = new Text();

	/**
	 * 输入文件： count key 一样 value count 的行
	 */
	@Override
	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
		//		String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();

		String s = line.toString();
		uid.set("1");
		mid.set(s);
		context.write(uid, mid);
	}

}
