package com.graduation;

import java.net.URI;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import nmf.CloudNMF;
import nmf.MatrixTrans;

public abstract class Gra_CloudNMF {

	public static void main(String[] args) throws Exception {
		//初始化数据
		//hadoop fs -rm /data/input/nmf_tmp
		//hadoop fs -rmr /data/NMF
		
		//nmf.CloudNMF -i hdfs:///data/apachelog/graduation/input/nmf.dat -o hdfs:///data/apachelog/graduation/input/nmf_tmp 
		//列是用户。。。。行是评分。
		//得出的H矩阵是倒着的，行其实是列也就是说，结果是7行2列，其实是2行7列。。。。
		//原始矩阵会被转置。。。也就是说，H的列数变成了V的列数了，应该是等于用户数。。。但是代码好像没有转置。。。我也是醉了
		//如果是稀疏矩阵就需要用这个
		//详情请看笔记本
		//String s="nmf.CloudNMF -i hdfs:///data/apachelog/graduation/input/nmf_tmp -o hdfs:///data/apachelog/graduation/output/NMF -m 5 -n 7 -k 2 -t 3 -r 5";
		//String arg="nmf.CloudNMF -i hdfs:///data/apachelog/graduation/input/nmf.dat -o hdfs:///data/apachelog/graduation/input/nmf_tmp ";
		////String s="nmf.CloudNMF -i hdfs:///data/input/nmf_tmp -o hdfs:///data/output/NMF -m 5 -n 7 -k 2 -t 3 -r 5";
		
		//hynamet01
		//String arg="nmf.CloudNMF -i hdfs:///data/apachelog/graduation/input/nmf1.txt -o hdfs:///data/apachelog/graduation/input/nmf2_tmp ";
		//String s="nmf.CloudNMF -i hdfs:///data/apachelog/graduation/input/nmf2_tmp -o hdfs:///data/apachelog/graduation/output/NMF2 -m 7 -n 5 -k 2 -t 3 -r 5";
		
		//由于代码是不进行转置的，所以需要设行为物品，列为用户,原始数据中行表示物品
		String arg="nmf.CloudNMF -i hdfs:///data/input/nmf1.txt -o hdfs:///data/input/nmf_tmp ";
		//由于代码是不进行转置的，所以需要设行为物品，列为用户,原始数据中行表示物品
		String s="nmf.CloudNMF -i hdfs:///data/input/nmf_tmp -o hdfs:///data/NMF -m 7 -n 5 -k 2 -t 3 -r 5";
		
		String[] a=s.split(" ");
		//将稀疏矩阵转化为稠密矩阵
		//初始数据
		//5			2			2			5			4			
		//3			2.5			0			0			3			
		//2.5		5			0			3			2			 
		//0			2			4			4.5			4			
		//0			0			4.5			0			3.5		
		//0			0			0			4			4			
		//0			0			5			0			0			
		MatrixTrans.main(arg.split(" "));//我们的需要这个，由于代码是不进行转置的，所以需要设行为物品，列为用户,原始数据中行表示物品
		
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(a[4]), conf);
		final Path outPath = new Path(a[4]);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}
		CloudNMF.main(a);
		System.out.println();
		AnalyseWH.main(null);
	}

}
