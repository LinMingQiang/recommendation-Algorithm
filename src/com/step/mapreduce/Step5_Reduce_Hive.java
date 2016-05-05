package com.step.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;

public class Step5_Reduce_Hive extends Reducer<Text, Text, NullWritable, HCatRecord>{
	private HCatSchema schema=null;
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		Iterator<Text> r=value.iterator();
		schema=HCatOutputFormat.getTableSchema(context.getConfiguration());
		String rc=r.next().toString();
		String user="";
		String movie="";
		if(!r.hasNext()){
			//去重，如果有两条数据，说明一条数从源数据来的，一条是从step4推荐结果来的
			//否则就执行这个
			String[] k=key.toString().split(":");
			HCatRecord record=new DefaultHCatRecord(schema.size());
			user=k[0];
			movie=k[1];
			record.setString("user", schema, user);
			record.setDouble("rep", schema, Double.parseDouble(rc));
			record.setString("mov", schema, movie);
			context.write(NullWritable.get(), record);
		}
	}
}
