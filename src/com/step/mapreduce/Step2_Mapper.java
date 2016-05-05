package com.step.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step2_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//输入数据1	101:5.0,102:3.0,103:2.5
		//输出数据这是一个用户的同现矩阵
		//		101:101 1   101:102 1    101:103 1
		//      102:101 1   102:102 1    102:103 1
		//		103:101 1   103:102 1    103:103 1
		String[] t = value.toString().split("\t");
		String[] tokens=t[1].split(",");
        for (int i = 0; i < tokens.length; i++) {
            String itemID = tokens[i].split(":")[0];
            for (int j = 0; j < tokens.length; j++) {
            	//为什么是从0开始而不是从i开始呢，因为推荐102的时候是这样的101  102,4
            	//那推荐101的时候102 101,4  这个102和101的同现应该一样，如果从i开始就会有102:101这样的同现,则需要从101:102转成102:101.这样麻烦
                String itemID2 = tokens[j].split(":")[0];
                context.write(new Text(itemID+":"+itemID2), new IntWritable(1));
            }
        }
	}
}
