package com.step;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.step.mapreduce.Step1_Mapper;
import com.step.mapreduce.Step1_Reduce;

public class Step1 {

	public static void main(String[] args) throws ClassNotFoundException, IOException, URISyntaxException, InterruptedException {
		// TODO Auto-generated method stub
		
	}
	public void run(Map<String, String> path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException{
		String INPUT_PATH = path.get("Step1_Input");
		String OUT_PATH = path.get("Step1_Output");			
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUT_PATH);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}
		Job job=new Job(conf);
		job.setJarByClass(Step1.class);//1.1ָ����ȡ���ļ�λ������
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setMapperClass(Step1_Mapper.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(Step1_Reduce.class);

		job.waitForCompletion(true);
		
	}

}
