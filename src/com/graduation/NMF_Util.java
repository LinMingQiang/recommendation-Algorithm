package com.graduation;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;

public class NMF_Util {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*String[][] a={{"11","12"},{"21","22"},{"31","32"},{"41","42"},{"51","52"}};
		for(String[] b:a){
			System.out.println();
			for(String c:b){
				System.out.print(c+" ");
			}
			
		}
		for(String[] b:new NMF_Util().ZhuanZhi(a)){
			System.out.println();
			for(String c:b){
				System.out.print(c+" ");
			}
			
		}*/
		
		new NMF_Util().Jianyan(null, null);
		
	}
	public static ArrayList<String[]> ZhuanZhi(ArrayList<String[]> maxie){
		int row=maxie.size();
		int colum=maxie.get(0).length;
		ArrayList<String[]> b=new ArrayList<String[]>();
		for(int i =0;i<colum;i++){
			String[] temp=new String[row];
			for(int j=0;j<row;j++){
				temp[j]=maxie.get(j)[i];
			}
			b.add(temp);
		}
		return b;
	}
	public static ArrayList<String[]> Jianyan(ArrayList<String[]> w,ArrayList<String[]> h){
		int row=w.size();
		int h_row=h.size();
		int colum=h.get(0).length;
		ArrayList<String[]> v=new ArrayList<String[]>();
		double re=0;
		NumberFormat   nf=new  DecimalFormat("0");
		for(int i =0 ;i<row;i++){
			String[] w_data=w.get(i);
			String[] v_data=new String[colum];
			for(int j=0;j<colum;j++){
				for(int m=0;m<h_row;m++){
					re=Double.parseDouble(w_data[m])*Double.parseDouble(h.get(m)[j])+re;
				}
				v_data[j]=""+nf.format(re);
				re=0;
			}
			v.add(v_data);
		}
		return v;
	}
	
	public static double cos(String[] a,String[] b){
		double tal_a=0;
		double tal_b=0;
		double tal_ab=0;
		double re=0.0;
		for(int i =0 ;i<a.length;i++){
			tal_a=tal_a+(Double.parseDouble(a[i])*Double.parseDouble(a[i]));
			tal_b=tal_b+(Double.parseDouble(b[i])*Double.parseDouble(b[i]));
			tal_ab=tal_ab+(Double.parseDouble(b[i])*Double.parseDouble(a[i]));
		}
		re=tal_ab/(Math.sqrt(tal_a)*Math.sqrt(tal_b));
		return re;
	}
}
