package com.graduation;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.step.Step1;
import com.step.Step2;
import com.step.Step3;
import com.step.Step4;
import com.step.Step5;

public class GraduationRun {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//记录运行时间
		long startTime=System.currentTimeMillis();
		String user="1";
		Map<String, String> path=new HashMap<String, String>();
		//HiveConnect h=new HiveConnect();
	    path.put("Step1_Input", "/data/input/ratings.dat");
		path.put("Step1_Output", "/data/output/step1");
		
		path.put("Step2_Input", path.get("Step1_Output"));
		path.put("Step2_Output", "/data/output/step2");
		
		 path.put("Step3_Input", path.get("Step1_Output"));
	     path.put("Step3_Output",  "/data/output/step3");
	    
	     path.put("Step4_Input1", path.get("Step3_Output"));
	     path.put("Step4_Input2", path.get("Step2_Output"));
	     
	     path.put("Step4_1_Output", "/data/output/step4/step4_1");
	     path.put("Step4_2_Output", "/data/output/step4/step4_2");
	     
	     path.put("Step5_output", "/data/output/step5");
	     //将表初始化
	    // h.init();
	     new Step1().run(path);//按用户分组，计算所有物品出现的组合列表，得到用户对物品的评分矩阵
		 new Step2().run(path);//对物品组合列表进行计数，建立物品的同现矩阵
	     new Step3().run(path);//得到评分矩阵
	     new Step4().run(path);//两矩阵想乘	    
	     new Step4().run2(path);//两矩阵相加
	     new Step5().run(path);//将结果存进HDFS
	     new Step5().runToHive(path);//将结果存进Hive
	   //service mysql start
	   //启动hive要启动Mysql作为元数据仓，
	     //hive --service metastore &
		//hive --service hiveserver2 &
	     //将结果存进Hive
	   // new Step5().runToHive(path);
	    //Hive排序....太浪费时间了，对所有用户都进行了排序，线下跑
	   /*	List<String> result=h.graSort(user,3,true);//用户，前n个推荐商品，是否已进hive，没有的话要建表
	   	for(String s:result){
	   		System.out.println(s);
	   	}*/
	   //把上面全部 都注释掉就是  实时查询的了，已经排序了
	   	/*Map<Double, String> result=h.runUnderLine(user);
	   	Set<Double> key = result.keySet();
	   	System.out.println("向用户 "+user+" 推荐的结果：");
	   	System.out.println("物品  :  推荐系数");
		for (Iterator<Double> it = key.iterator(); it.hasNext();) {
			Double s = it.next();
			System.out.println(result.get(s)+"  :  "+s);
		}*/
		
		//计算时间
	   	long endTime=System.currentTimeMillis();
		   long time=(endTime-startTime);
		   System.out.println("共花费时间:"+time/1000.0);
		   System.exit(0);
	}
	
	
}
