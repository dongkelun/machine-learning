package com.nowcoder.course;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable ONE = new IntWritable(1);
	private Text uid = new Text();

	@Override
	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
		String keys = line.toString();
		String[] split = keys.split("\t");
		String[] split2 = split[1].split(";");
		if (split2.length > 1) {
			uid.set(split[0]);
			for (int i = 0; i < split2.length - 1; i++) {

				for (int j = i + 1; j < split2.length; j++) {
					if (Integer.parseInt(split2[i]) < Integer.parseInt(split2[j])) {
						uid.set(split2[i] + "_" + split2[j]);
					} else {
						uid.set(split2[j] + "_" + split2[i]);
					}
					context.write(uid, ONE);
				}
			}
		}

	}
}
