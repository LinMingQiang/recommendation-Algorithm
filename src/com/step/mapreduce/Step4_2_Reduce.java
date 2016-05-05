package com.step.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step4_2_Reduce extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		//		求和
		//输入数据：	1	102,12.0  给1推荐102的系数要相加
		//			1	107,18.0
		
		//输出数据： 	1   102,总系数
		//			1   107,总系数
		 Map<String,Double> map = new HashMap<String,Double>();// 结果
         for (Text line : value) {
             String[] tokens = line.toString().split(",");
             String itemID = tokens[0];//推荐的物品
             Double score = Double.parseDouble(tokens[1]);//推荐系数
              if (map.containsKey(itemID)) {
                  map.put(itemID,map.get(itemID)+score);// 矩阵乘法结果求和计算
              } else {
                  map.put(itemID, score);
              }
         }
         
         Iterator iter = map.keySet().iterator();
         while (iter.hasNext()) {
             String itemID = (String)iter.next();
             double score = map.get(itemID);
             Text v = new Text(itemID + "," + score);
             context.write(key, v);
         }
     }
}
