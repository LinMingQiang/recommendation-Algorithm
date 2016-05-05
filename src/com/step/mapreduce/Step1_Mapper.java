package com.step.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step1_Mapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//初始数据 1	101	5.0  
		//输出数据 1   101:5.0
		if(value.toString().length()>0){
			String[] tokens = value.toString().split("\t");
			if(tokens.length>=3){
				int userID = Integer.parseInt(tokens[0]);
		        String itemID = tokens[1];
		        String pref = tokens[2];
		        context.write(new IntWritable(userID), new Text(itemID+":"+pref));
			}
	        
		}
		
	}
}
