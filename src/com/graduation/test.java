package com.graduation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class test {
	private static ArrayList<String[]> h_data;
	private static ArrayList<String[]> w_data;
	private static ArrayList<String[]> v_data;
	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		test awh=new test();
		awh.get_H();	//得出H矩阵
		awh.get_W();
		awh.prinMaxin(h_data);
		System.out.println();
		awh.prinMaxin(w_data);
		h_data=NMF_Util.ZhuanZhi(h_data);
		v_data=NMF_Util.Jianyan(w_data, h_data);
		v_data=NMF_Util.ZhuanZhi(v_data);
		awh.prinMaxin(v_data);
	}
	public void get_H() throws FileNotFoundException{
		File inputFile = new File("C:\\Users\\Administrator\\Desktop\\MyStudy\\课程设计\\data\\NMF\\h_advance.txt");
		FileInputStream in = new FileInputStream(inputFile);
		BufferedReader br = null;
		String temp = null;
		h_data=new ArrayList<String[]>();
		try {
			{
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
	public void get_W() throws FileNotFoundException{
		File inputFile = new File("C:\\Users\\Administrator\\Desktop\\MyStudy\\课程设计\\data\\NMF\\w_advance.txt");
		FileInputStream in = new FileInputStream(inputFile);
		BufferedReader br = null;
		String temp = null;
		w_data=new ArrayList<String[]>();
		try {
			{
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
	
}
