package com.step;

import java.io.IOException;
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

import com.step.mapreduce.Step3_Mapper;

public class Step3 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	public void run(Map<String, String> path) throws Exception{
		//得到评分矩阵
		String INPUT_PATH = path.get("Step3_Input");
		String OUT_PATH = path.get("Step3_Output");			
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUT_PATH);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}
		Job job=new Job(conf);
		job.setJarByClass(Step3.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setMapperClass(Step3_Mapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.waitForCompletion(true);
		
	}
}
