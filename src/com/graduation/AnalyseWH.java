package com.graduation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AnalyseWH {
	private static ArrayList<String[]> h_data;
	private static ArrayList<String[]> w_data;
	private static ArrayList<String[]> v_data;
	private static ArrayList<Double[]> sor_re;
	private static Double[] result;
	private static ArrayList<Double[]> sor_result;
	private static int a=1;//用户为1，第一个
	private static int NUk=4;//k要小于等于用户数-1（减去自身）
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AnalyseWH awh=new AnalyseWH();
		awh.get_H();	//得出H矩阵
		awh.get_W();
		awh.get_V();//获取源矩阵
		v_data=NMF_Util.ZhuanZhi(v_data);//因为后面要获取每个用户的评分向量，
										//如果没有转置的话会很麻烦，转置后变成每一行表示每个用户的评分向量
		//awh.prinMaxin(v_data);
		//计算用户之间的相似性
		//Sim(u,i)=cos(□((W_u ) ⃗ ),□((W_i ) ⃗ ))
		awh.cos();
		/*for(Double[] bb:sor_re){
		 //临近用户
			System.out.println((Integer.parseInt(new DecimalFormat("0").format(bb[0]))+1)+"\t"+bb[1]);
		}*/
		System.out.println();
		//根据相近用户的评分，根据公式得出用户a对商品的预测评分
		//(r_an ) ̂=(r_a ) ̅+(∑_(u_k∈NU_a)▒
		//〖Sim(u_a,u_k )×(r_kn-(r_k ) ̅)〗)/(∑_(u_k∈NU_a)▒|Sim(u_a,u_k )| )
		awh.getResult();
		//对预测评分进行排序
		sor_result=sort(result);
		String[] ra=v_data.get(a-1);//获取用户a的所有评分
		System.out.println("向用户 "+a+" 推荐的物品列表为");
		System.out.println("物品名\t"+"预测评分");
		for(Double[] bb:sor_result){
			int index=Integer.parseInt(new DecimalFormat("0").format(bb[0]));
			if(Double.parseDouble(ra[index])<=0){
				//表示没看过，输出预测评分
				System.out.println((index+1)+"\t"+bb[1]);
			}
			
		}
		
		
		//h_data=NMF_Util.ZhuanZhi(h_data);//进行转置,用于验证
		/*awh.prinMaxin(h_data);
		System.out.println("<<<<<<<<<<<<<<<<<<<<<<");
		awh.prinMaxin(w_data);
		System.out.println("<<<<<<<<<<<<<<<<<<<<<<");*/
		
		
		//v_data=NMF_Util.Jianyan(w_data, h_data);//效验W*H
		//awh.prinMaxin(v_data);
		//System.out.println("<<<<<<<<<<<<<<<<<<<<<<");
		
	
		
		
		
	}
	public void get_V(){
		//Path inputFile = new Path("/data/apachelog/graduation/input/nmf1.txt");
		
		Path inputFile = new Path("/data/input/nmf1.txt");
		
		InputStream in = null;
		BufferedReader br = null;
		String temp = null;
		Configuration conf=new Configuration();
		 v_data=new ArrayList<String[]>();
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(inputFile)) {
				in = fileSystem.open(inputFile);
				br = new BufferedReader(new InputStreamReader(in));
				while ((temp = br.readLine()) != null) {
					String[] data=temp.split("\t");
					v_data.add(data);
					}
				}
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	public void get_H(){
		//根据矩阵W，计算出目标用户Ua对项目的评分向量对基矩阵W的投影向量为Ha（投影矩阵H的第a列）
		//计算Ha与投影矩阵H各列之间的相似度，采用余弦相似性，将相似度最高的k个用户组成活跃用户Ua的最近邻集合，设为NUa
		//Path inputFile = new Path("/data/apachelog/graduation/output/NMF2/h_advance.txt");
		
		Path inputFile = new Path("/data/NMF/h_advance.txt");
		
		InputStream in = null;
		BufferedReader br = null;
		String temp = null;
		Configuration conf=new Configuration();
		 h_data=new ArrayList<String[]>();
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(inputFile)) {
				in = fileSystem.open(inputFile);
				br = new BufferedReader(new InputStreamReader(in));
				while ((temp = br.readLine()) != null) {
					String[] data=temp.split("\t")[1].toString().split(",");
					h_data.add(data);
					}
				}
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	public void get_W(){
		//Path inputFile = new Path("/data/apachelog/graduation/output/NMF2/w_advance.txt");
		
		Path inputFile = new Path("/data/NMF/w_advance.txt");
		
		InputStream in = null;
		BufferedReader br = null;
		String temp = null;
		Configuration conf=new Configuration();
		 w_data=new ArrayList<String[]>();
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			if (fileSystem.exists(inputFile)) {
				in = fileSystem.open(inputFile);
				br = new BufferedReader(new InputStreamReader(in));
				while ((temp = br.readLine()) != null) {
					String[] data=temp.split("\t")[1].toString().split(",");
					w_data.add(data);
					}
				}
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
	}

	public void prinMaxin(ArrayList<String[]> maxin){
		for(String[] aa:maxin){
			System.out.println();
			for(String a:aa){
				System.out.print(a+" ");
			}
		}
	}
	
	public void cos(){
		//找出前K个与用户a相近的
		//i表示第几个用户
		String[] ua=h_data.get(a-1);
		
		Double[] re=new Double[(h_data.size())];
		for(int i =0 ;i<h_data.size();i++){
			//余弦相似性，i表示Ui
			if(i!=a-1){
				//按行来乘的，由于H的行就是列（因为没有转置，H本来是需要按列来乘的）
				re[i]=NMF_Util.cos(ua, h_data.get(i));
		
			}else{
				re[i]=-1.0;
				
			}
		}
		
		int index=0;
		 sor_re=new ArrayList<Double[]>();
		int i =NUk;//这里可以条参数的，如果是要选k个的话，应该条件变成
		while(i>0){
			//排序，按理应该是i>=0才是正常的排序，但自身是不需要参与排序的,找出re.length-1个与a用户相近的
			//re中也包括了自己，所以排序的时候要去掉自己
				Double[] max=new Double[2];
				max[0]=(double) 0;
				max[1]=re[0];
				for(int j=1;j<h_data.size();j++){
					//找出最大的
					if(re[j]>0){//等于0的是比较过的，不参与再比较，还有自身是小于0的要跳过，所以得出的结果不会有自己
						if(re[j]>max[1]){
							max[0]=(double) j;
							max[1]=re[j];
						}
					}
					
				}
				index=Integer.parseInt(new DecimalFormat("0").format(max[0]));
				re[index]=0.0;
				sor_re.add(max);
			i--;
		}
//		Arrays.sort(re)
	}

	public void getResult(){
		double r_a=0.0;//用户Ua对所有项目的评分的平均分
		double r_k=0.0;//用户Uk对所有项目的评分的平均分
		double rkn=0.0;//用户Uk对项目n的评分
		double ran=0.0;//用户Ua对项目n的预测评分
		double sim_Ua_Uk=0.0;//用户Ua和用户Uk的相似度
		double E_fenmu=0.0;
		double E_fenzi=0.0;
		//公式：E表示相加
		// ran=ra_ + E(sim_Ua_Uk*(rkn-rk_))/E(|sim_Ua_Uk|)
		int Inum=v_data.get(0).length;//项目数
		String[] ra=v_data.get(a-1);//获取用户a的所有评分
		r_a=getAverage(ra,Inum);//获取平局分
		 result=new Double[Inum];
		for(int n=0;n<Inum;n++){
			//项目数
			for(int i=0;i<sor_re.size();i++){
			//用户数K	(按相似度的大小排序，高相似度的开始)
			int	k=Integer.parseInt(new DecimalFormat("0").format(sor_re.get(i)[0]));//获取用户相似度排序k的用户的id
				String[] rk=v_data.get(k);//用户k的对所有的项目评分
				sim_Ua_Uk=sor_re.get(i)[1];//相似度
				rkn=Double.parseDouble(rk[n]);//用户Uk对n项目的评分
				r_k=getAverage(rk,Inum);//用户对所有项目的平均分
				E_fenmu=E_fenmu+(sim_Ua_Uk)*(rkn-r_k);//分母
				E_fenzi=E_fenzi+sim_Ua_Uk;
			}
			
			rkn=r_a+E_fenmu/E_fenzi;
			result[n]=rkn;
		}
	}
	public double getAverage(String[] goal,int Inum){
		double goals=0.0;
		for(int raa=0;raa<goal.length;raa++){
			goals=Double.parseDouble(goal[raa])+goals;
		}
		return goals/Inum;
	}
	
	public static ArrayList<Double[]> sort(Double[] re){
		int index=0;
		ArrayList<Double[]> sot_re=new ArrayList<Double[]>();
		int i =re.length-1;
		while(i>=0){
			//排序
			
				Double[] max=new Double[2];
				max[0]=(double) 0;
				max[1]=re[0];
				for(int j=1;j<re.length;j++){
					//找出最大的
					if(re[j]>0){//等于0的是比较过的，不参与再比较
						if(re[j]>max[1]){
							max[0]=(double) j;
							max[1]=re[j];
						}
					}
					
				}
				index=Integer.parseInt(new DecimalFormat("0").format(max[0]));
				re[index]=0.0;
				sot_re.add(max);
			i--;
		}
		return sot_re;
	}
	
}
