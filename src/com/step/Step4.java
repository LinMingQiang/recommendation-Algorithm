package com.step;
import java.net.URI;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.step.mapreduce.*;

import com.step.mapreduce.Step4_Mapper;
import com.step.mapreduce.Step4_Reduce;

public class Step4 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	/**
	 * @deprecated 同现矩阵与评分矩阵想乘和相加
	 * @author 林明强
	 */
	public void run(Map<String, String> path) throws Exception{
		//同现矩阵和评分矩阵想乘
		String INPUT_PATH = path.get("Step4_Input1");
		String INPUT_PATH2=path.get("Step4_Input2");
		String OUT_PATH = path.get("Step4_1_Output");			
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUT_PATH);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}
		Job job=new Job(conf);
		job.setJarByClass(Step4.class);
		
		//FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileInputFormat.addInputPaths(job, INPUT_PATH);
		FileInputFormat.addInputPaths(job, INPUT_PATH2);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setMapperClass(Step4_Mapper.class);
		job.setReducerClass(Step4_Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.waitForCompletion(true);
		
	}

	public void run2(Map<String, String> path) throws Exception{
		//结果相加
		String INPUT_PATH = path.get("Step4_1_Output");
		String OUT_PATH = path.get("Step4_2_Output");			
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUT_PATH);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}
		Job job=new Job(conf);
		job.setJarByClass(Step3.class);
		
		FileInputFormat.addInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setMapperClass(Step4_2_Mapper.class);
		job.setReducerClass(Step4_2_Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}
