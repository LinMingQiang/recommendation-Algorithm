package com.step.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step4_2_Mapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//输入数据：	1	102,2.0 给3推荐107的 系数值
		//输出		1	102,2.0
		String[] t = value.toString().split("\t");
		String[] tokens=t[1].split(",");
        Text k = new Text(t[0]);//用户
        Text v = new Text(tokens[0]+","+tokens[1]);
        context.write(k, v);
	}
}
