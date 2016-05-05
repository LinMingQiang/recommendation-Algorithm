package com.step.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text>{
	private String flag=null;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//有两个输入路径
		 FileSplit split = (FileSplit) context.getInputSplit();
         flag = split.getPath().getParent().getName();// 判断读的数据集
         
	}
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//数据输入：	101:101	5			// 同现矩阵
		//	 		101:102	3
		//输出数据：	101 A:101,5		//推荐101
		//			101 A:102,3		//推荐102
		  String[] tokens = value.toString().split("\t");
          if (flag.equals("step2")) {// 同现矩阵
              String[] v1 = tokens[0].split(":");
              String itemID1 = v1[0];//电影1
              String itemID2 = v1[1];//电影2
              String num = tokens[1];//一起出现的次数

              Text k = new Text(itemID1);
              Text v = new Text("A:" + itemID2 + "," + num);
              //以前面的为主键输出
              context.write(k, v);

          } else if (flag.equals("step3")) {// 评分矩阵
        	//输入数据101 1:5.0		// 评分矩阵
      		//		102 1:3.0
      		//		103 1:2.5
        	 //输出 	101 B:1,5.0
        	  //	102 B:1,3.0
        	  //	103 B:1,2.5
              String[] v2 = tokens[1].split(":");
              String itemID = tokens[0];//电影
              String userID = v2[0];//用户
              String pref = v2[1];//评分
              Text k = new Text(itemID);
              Text v = new Text("B:" + userID + "," + pref);
              context.write(k, v);
          }
      }
}
