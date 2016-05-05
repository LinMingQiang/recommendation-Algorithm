package com.graduation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class HiveConnect {
	public static Connection conn=null;
	public static void main(String[] args) {
		HiveConnect h=new HiveConnect();
		//在linux里面开启hive服务
		//先起mysql》  service mysql start
		//hive --service metastore &       启动hive这个很重要，启动他的数据源
		//hive --service hiveserver2 &    &是表示在后台运行
		//h.create();
	}
	static{
		try {

			if(conn==null){
				Class.forName("org.apache.hive.jdbc.HiveDriver");
				//有权限问题就用root登陆
				conn = DriverManager.getConnection(
						"jdbc:hive2://gra-1:10000/default", "root", "hadoop");
				System.out.println("Connect To Hive Success！");
			}
			
		}catch(Exception e){
			e.printStackTrace();
			
		}
	}
//线下跑
	public void graCreateTable(){
		Connection conn=HiveConnect.conn;
		if(conn!=null){
			String drop="drop table test.graduation";
			String loadData="create EXTERNAL table test.graduation(user string,rep double,mov string) row format delimited fields terminated by '\\t' Location '/data/output/step5' ";
			//String table="insert overwrite table test.graduation select user,rep,mov from test.graduation group by user,rep,mov order by user,rep desc"; 
			
			try{
				Statement stmt = conn.createStatement();
				stmt.execute(drop);
				stmt.execute(loadData);
				//stmt.execute(table);
			}catch(Exception e){
				e.printStackTrace();
			}
		}else{
			System.out.println("连接失败！");
		}
	}
	public List<String> graSort(String user,int n,boolean hive){
		//指定前N个
		List<String> result = new ArrayList<String>();
		Connection conn=HiveConnect.conn;
		if(conn==null){
			return null;
		}
		if(!hive){
			graCreateTable();
		}
		//排序
		String sort="insert overwrite table test.graduation select user,rep,mov from test.graduation group by user,rep,mov order by user,rep desc"; 
		//查询
		String select="select * from test.graduation where user='"+user+"' limit "+n;
		try{
			Statement stmt = conn.createStatement();
			
			stmt.execute(sort);
			ResultSet rs=stmt.executeQuery(select);
			while (rs.next()) {
				result.add(rs.getString(1)+"\t"+rs.getDouble(2)+"\t"+rs.getString(3));
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
		
	}
	
	
	
//上面暂时用不到	
	public void init(){
		Connection conn=HiveConnect.conn;
		if(conn!=null){
			String truncate="truncate table test.graduation";
			//String table="drop table test.graduation"; 
			//String create="create table test.graduation(user string,rep double,mov string) row format delimited fields terminated by '\\t'";
			try{
				Statement stmt = conn.createStatement();
				stmt.execute(truncate);
				//stmt.execute(table);
				//stmt.execute(create);
			}catch(Exception e){
				e.printStackTrace();
			}
		}else{
			System.out.println("连接失败！");
		}
	}

	public Map<Double, String> runUnderLine(String user){
		Map<Double, String> result=new TreeMap<Double, String>(new Comparator<Object>(){ 
           public int compare(Object o1, Object o2) {return o2.hashCode()-o1.hashCode(); }});
		Connection conn=HiveConnect.conn;
		String excut="select * from test.graduation where user='"+user+"'";
		try{
			Statement stmt = conn.createStatement();
			
			stmt.execute(excut);
			ResultSet rs=stmt.executeQuery(excut);
			while (rs.next()) {
				result.put(rs.getDouble(2), rs.getString(3));
			}
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
		return result;
	}
}
