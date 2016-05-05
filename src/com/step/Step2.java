package com.step;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.step.mapreduce.*;

public class Step2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	}
	public void run(Map<String, String> path) throws Exception{
		//计算同现矩阵
		//输出数据 101:103 (多个用的1相加如 18 表示101:103 一同出现了18次) 
		String INPUT_PATH = path.get("Step2_Input");
		String OUT_PATH = path.get("Step2_Output");			
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUT_PATH);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}
		Job job=new Job(conf);
		job.setJarByClass(Step2.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, outPath);

		job.setMapperClass(Step2_Mapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(Step2_Reduce.class);

		job.waitForCompletion(true);
		
	}

}
