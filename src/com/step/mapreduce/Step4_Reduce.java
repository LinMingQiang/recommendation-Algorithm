package com.step.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step4_Reduce extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		//key为电影ID
         Map<String,String> mapA = new HashMap<String,String>();
         Map<String,String> mapB = new HashMap<String,String>();

         for (Text line : value) {
             String val = line.toString();
             if (val.startsWith("A:")) {
            	 //原理：  101和102一同出现的次数决定了101和102的相似程度，我们是要将102推荐给用户。
            	 //     要将102推荐给用户，我们需要知道102 和101（103,或者     其他的    ）一同出现的频率
            	 //		和用户对101（103，或者     其他的  ）的评分
            	 //		然后将 系数乘与评分    再相加      其他的    的积 最终得出给用户推荐102的系数分
            	 // 即，当前只是依据用户对101的评分和101和推荐物品 同现频率来对用户做推荐，之后依据102,103，。。所有的
             	//输入数据：	101 A:101,5 // 推荐101的
         		//			101 A:102,3	// 推荐102的
            	 //        	101 A:104,11 //推荐104的
                 String[] kv = val.substring(2).split(",");
                 mapA.put(kv[0], kv[1]);
             } else if (val.startsWith("B:")) {
            	 //1对101的评分，给1推荐102的系数为，对101的评分（评分）*101和102一起出现的次数（同现）
            	//输入	101 B:1,5.0 1对101的评分    给1 推荐101，推荐102，推荐104。。根据上面A后面的物品ID推荐
           	  //		101 B:2,3.5 2对101的评分	给2推荐101，推荐102，推荐104
           	  //		101 B:3,4.0	3对101的评分
            	 
            	 //输出 	3	107,2.0  给3推荐107的系数是2.0
            	 //		2	107,2.0
            	 //		1	107,5.0
                 String[] kv =val.substring(2).split(",");
                 mapB.put(kv[0], kv[1]);

             }
         }
         double result = 0;
         Iterator iter = mapA.keySet().iterator();
         while (iter.hasNext()) {
             String mapk = (String)iter.next();// itemID  推荐给用户的物品

             int num = Integer.parseInt(mapA.get(mapk));// 推荐物品和用户的评分物品一同出现的频率 
             Iterator iterb = mapB.keySet().iterator(); 
             while (iterb.hasNext()) {
                 String mapkb = (String)iterb.next();// userID 用户的ID
           
                 double pref = Double.parseDouble(mapB.get(mapkb));// 用户对物品的评分
                 result = num * pref;// 矩阵乘法相乘计算

                 Text k = new Text(mapkb);
                 Text v = new Text(mapk + "," + result);
                 context.write(k, v);
                 //输出：  用户ID  推荐的物品ID，推荐系数
             }
         }
	}
}
