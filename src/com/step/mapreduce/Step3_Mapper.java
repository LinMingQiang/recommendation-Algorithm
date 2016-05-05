package com.step.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step3_Mapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//输入数据 1	101:5.0,102:3.0,103:2.5
		//输出数据101 1:5.0
		//		102 1:3.0
		//		103 1:2.5
		//		评分矩阵
		String[] t=value.toString().split("\t");
		String[] tokens = t[1].split(",");
        for (int i = 0; i < tokens.length; i++) {
            String[] vector = tokens[i].split(":");
            String itemID = vector[0];
            String pref = vector[1];
            context.write(new Text(itemID), new Text(t[0] + ":" + pref));
        }
        
	}
}
