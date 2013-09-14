package com.test.hbase;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class MakeTabData {
	private static long NUM_ROWS=1000000L;
	
	
	public static void main(String []args){
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter("/home/dc/tabfile.txt"));
			Integer numRows = 0;
			
			for(int i=0;i<NUM_ROWS;i++){
				bw.write(numRows+"\t"+"col1"+numRows+"\t"+"col2"+numRows);
				bw.newLine();
				numRows++;
			}
			bw.close();
			
			System.out.println("size file:"+(new File("/home/dc/tabfile.txt")).length()/(1024*1024)+" mbs");
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	
	
}
