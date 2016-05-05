package com.step.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step1_Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
	@Override
	protected void reduce(IntWritable key, Iterable<Text> value,
			Reducer<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//输入数据 1   101:5.0
		//输出数据1	101:5.0,102:3.0,103:2.5
		StringBuffer sb=new StringBuffer();
		for(Text t:value){
			sb.append(","+t.toString());
		}
		
		context.write(key, new Text(sb.toString().replaceFirst(",", "")));
	}
}
