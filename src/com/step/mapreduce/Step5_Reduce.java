package com.step.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;

public class Step5_Reduce extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		Iterator<Text> r=value.iterator();
		String rc=r.next().toString();
		String user="";
		String movie="";
		if(!r.hasNext()){
			//没有相同的用户Key，表示在input和Step4里面没有相同的	用户:物品，达到去重的目的
			String[] k=key.toString().split(":");
			user=k[0];
			movie=k[1];
			context.write(new Text(user+"\t"+rc), new Text(movie));
		}
	}
}
