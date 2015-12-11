package com.nowcoder.course;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer4 extends Reducer<Text, Text, Text, Text> {
	private final static IntWritable ZERO = new IntWritable(0);
	Double a = new Double(0);
	Double b = new Double(0);
	Double c = new Double(0);
	Text result = new Text();
	String s[] = new String[2];

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int i = 0;
		for (Text val : values) {
			s[i++] = val.toString();
		}
		if (i == 1) {
			result.set(ZERO.toString());
			context.write(key, result);
		} else {
			for (String s1 : s) {
				if (s1.toString().contains(":")) {
					String[] split = s1.toString().split(":");

					a = Double.parseDouble(split[0]);
					b = Double.parseDouble(split[1]);
				} else {
					c = Double.parseDouble(s1.toString());
				}
			}
			if (a != 0 && b != 0 && c != 0) {
				c = c / (a + b - c);
				result.set(c.toString());
				context.write(key, result);
			}
		}

	}
}
