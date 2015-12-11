package com.nowcoder.course;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
	private IntWritable mid = null;
	private Text uid = new Text();
	@Override
	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
		String keys = line.toString();
		String[] split = keys.split("\t");
		mid = new IntWritable(Integer.parseInt(split[0]));
		for (String s : split[1].split(";")) {
			uid.set(s);
			context.write(uid, mid);
		}

	}

}
