package com.nowcoder.course;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper4 extends Mapper<Object, Text, Text, Text> {
	private Text uid = new Text();
	private Text mid = new Text();

	@Override
	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
		String[] split = line.toString().split("\t");
		uid.set(split[0]);
		mid.set(split[1]);
		context.write(uid, mid);
	}

}