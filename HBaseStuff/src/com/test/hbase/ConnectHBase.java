package com.test.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ConnectHBase {
	private static String TABLE_NAME="testtable1";
	private static Integer NUM_ROWS=100;
	
	private static void addRows(){
		for(int i=0;i<NUM_ROWS;i++){
			
		}
	}
	
	public static void main(String []args){
		try{
			HBaseConfiguration config = new HBaseConfiguration();
			//for htable...are these 2 configs the same?
			Configuration hconf = HBaseConfiguration.create();
			//this is one client. add multiple clients in mappers. 
			HBaseAdmin admin = new HBaseAdmin(config);	
			
			if(admin.tableExists(TABLE_NAME)){
				System.out.println("table exists");
			}else{
				String []family={"cf1"};
				HTableDescriptor tableDesc = new HTableDescriptor(TABLE_NAME);
	            for (int i = 0; i < family.length; i++) {
	                tableDesc.addFamily(new HColumnDescriptor(family[i]));
	            }
	            admin.createTable(tableDesc);
	            System.out.println("create table " + TABLE_NAME + " ok.");
			}
			
			System.out.println("testtable avail:"+admin.isTableAvailable("testtable"));
			//addrows
			HTable htable = new HTable(hconf,TABLE_NAME);
			for(int i=0;i<NUM_ROWS;i++){
				Put put = new Put(Bytes.toBytes(new Integer(i).toString()));
				put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(i));
				htable.put(put);
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
}
