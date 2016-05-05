package com.step.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Step5_Mapper extends Mapper<LongWritable, Text, Text, Text>{
	private String flag=null;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		 FileSplit split = (FileSplit) context.getInputSplit();
         flag = split.getPath().getParent().getName();// 判断读的数据集
	}
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//输入数据： 	1   102,总系数
		//			1   107,总系数
		//输出数据：      1:102      总系数 
		//			1:107      总系数
		if(flag.equals("step4_2")){
			//1	107,5.0
			String[] r=value.toString().split("\t");
			String[] rdata=r[1].split(",");
			context.write(new Text(r[0]+":"+rdata[0]), new Text(rdata[1]));
		}else if(flag.equals("input")){
			//去重，如果用户已经买过这个物品了，就去掉
			//		1	101	5.0
			//输出数据 1:101 5.0
			String[] data=value.toString().split("\t");
			context.write(new Text(data[0]+":"+data[1]), new Text(data[2]));
		}
	}
	
	
	
	
	
	
	
	
	
	
}
