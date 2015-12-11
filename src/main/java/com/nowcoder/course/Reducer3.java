package com.nowcoder.course;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3 extends Reducer<Text, Text, Text, Text> {
	private Text uid = new Text();
	private Text mid = new Text();
	ArrayList<String> list = new ArrayList<>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text val : values) {
			list.add(val.toString());
		}
		for (int i = 0; i < list.size() - 1; i++) {
			String[] split1 = list.get(i).toString().split("\t");
			for (int j = i + 1; j < list.size(); j++) {
				String[] split2 = list.get(j).toString().split("\t");
				if (Integer.parseInt(split1[0]) < Integer.parseInt(split2[0])) {
					uid.set(split1[0] + "_" + split2[0]);
					mid.set(split1[1] + ":" + split2[1]);
				} else {
					uid.set(split2[0] + "_" + split1[0]);
					mid.set(split2[1] + ":" + split1[1]);
				}
				context.write(uid, mid);
			}
		}
	}
}
