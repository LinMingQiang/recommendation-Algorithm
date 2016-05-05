package com.step.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step2_Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//输入数据101:101 1   101:102 1    101:103 1
		//      102:101 1   102:102 1    102:103 1
		//		103:101 1   103:102 1    103:103 1
		//多个用户有多个同现矩阵，现在要合并成一个同现矩阵
		//
		//输出数据 101:103 (多个用的1相加如 18 表示101:103 一同出现了18次) 
		int sum=0;
		for(IntWritable i:value){
			sum=sum+i.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
