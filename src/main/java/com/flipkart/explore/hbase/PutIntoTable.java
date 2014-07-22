package com.flipkart.explore.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutIntoTable {

	public static void main(String[] args) throws IOException{
		Configuration conf=HBaseConfiguration.create();
		HTable table=new HTable(conf, "testtable");
		System.out.println("Auto flush: "+table.isAutoFlush());
		
		table.setAutoFlush(false);
		
		Put put=new Put(Bytes.toBytes("row1"));
		put.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),Bytes.toBytes("val1"));
		table.put(put);
		
		Put put2=new Put(Bytes.toBytes("row2"));
		put2.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),Bytes.toBytes("val2"));
		table.put(put2);
		
		Get get=new Get(Bytes.toBytes("row1"));
		Result res=table.get(get);
		System.out.println("Result before flush: "+res);
		
		table.flushCommits();
		
		res=table.get(get);
		System.out.println("Result after flush: "+res);
	}
}