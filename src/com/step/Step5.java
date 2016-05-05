package com.step;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import com.step.mapreduce.*;

public class Step5 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	/**
	 * @deprecated 将推荐中的用户看过的电影进行剔除
	 * @author 林明强
	 * @throws URISyntaxException 
	 * @throws Exception 
	 */
	public void run(Map<String, String> path) throws Exception{
		String INPUT_PATH = path.get("Step1_Input");//原始用户 电影 评分 数据
		String INPUT_PATH2=path.get("Step4_2_Output");//Step4结果推荐数据
		String OUT_PATH = path.get("Step5_output");
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUT_PATH);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}
		Job job=new Job(conf);
		job.setJarByClass(Step5.class);
		
		//FileInputFormat.setInputPaths(job, INPUT_PATH);
				FileInputFormat.addInputPaths(job, INPUT_PATH);
				FileInputFormat.addInputPaths(job, INPUT_PATH2);
				FileOutputFormat.setOutputPath(job, outPath);
				
				job.setMapperClass(Step5_Mapper.class);
				job.setReducerClass(Step5_Reduce.class);
				
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);

				job.waitForCompletion(true);
	}
	public void runToHive(Map<String, String> path) throws Exception{
		String INPUT_PATH = path.get("Step1_Input");//原始用户 电影 评分 数据
		String INPUT_PATH2=path.get("Step4_2_Output");//Step4结果推荐数据
		Configuration conf = new Configuration();
		Job job=new Job(conf);
		job.setJarByClass(Step5.class);
		
		//FileInputFormat.setInputPaths(job, INPUT_PATH);
				FileInputFormat.addInputPaths(job, INPUT_PATH);
				FileInputFormat.addInputPaths(job, INPUT_PATH2);
				//FileOutputFormat.setOutputPath(job, outPath);
				
				job.setMapperClass(Step5_Mapper.class);
				job.setReducerClass(Step5_Reduce_Hive.class);
				//要启动Mysql作为元数据仓，hive --service metastore启动hive
				org.apache.hive.hcatalog.mapreduce.HCatOutputFormat.setOutput(job, OutputJobInfo.create("test", "graduation", null));
				org.apache.hive.hcatalog.data.schema.HCatSchema schema=org.apache.hive.hcatalog.mapreduce.HCatOutputFormat.getTableSchema(job.getConfiguration());
				org.apache.hive.hcatalog.mapreduce.HCatOutputFormat.setSchema(job, schema);
				
				job.setOutputFormatClass(org.apache.hive.hcatalog.mapreduce.HCatOutputFormat.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);

				job.waitForCompletion(true);
	}
	
}
