package com.peng;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
* @ClassName: HBaseHelper 
* @Description: 操作HBase的工具类
* @author pengcq 412940226@qq.com 
* @date 2017年9月23日 下午5:20:16 
*
 */
public class HBaseHelper {
	private static final Logger logger=LoggerFactory.getLogger(HBaseHelper.class);
	/*HBase的配置对象，启动的时候会从classpath中加载对应的配置信息*/
	public static Configuration configuration=HBaseConfiguration.create();
	/**
	 * 创建表
	 * @param tableName
	 * @param columnFamily
	 */
	public static void createTable(String tableName, String[] columnFamily){
		try {
			HBaseAdmin hBaseAdmin=new HBaseAdmin(configuration);
			if (hBaseAdmin.tableExists(tableName)) {
				logger.info("["+tableName+"]已经存在！");
			}else {
				HTableDescriptor hTableDescriptor=new HTableDescriptor(TableName.valueOf(tableName));
				for(String cloumn:columnFamily){
					hTableDescriptor.addFamily(new HColumnDescriptor(cloumn));
				}
				hBaseAdmin.createTable(hTableDescriptor);
				logger.info("["+tableName+"]创建成功！");
			}
			hBaseAdmin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	/**
	 * 添加一条数据
	 * @param tableName
	 * @param rowKey 唯一主键
	 * @param columnFamily 列簇值对象数组，一张表可能包含多个列簇
	 */
	public static void addRow(String tableName, String rowKey,ColumnFamilyValue[] columnFamily){
		try {
			HTable hTable=new HTable(configuration, tableName);
			Put put=new Put(Bytes.toBytes(rowKey));
			logger.info("put rowKey start================="+rowKey+"==================");
			for (int i = 0; i < columnFamily.length; i++) {
				columnFamily[i].put(put);
			}
			hTable.put(put);
			hTable.close();
			logger.info("put rowKey end==================="+rowKey+"==================");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	/**
	 * 返回一个表的全部数组，这个操作在生产上不会有，这里就是为了测试
	 * @param hTable
	 * @return
	 */
	public static ResultScanner scanALL(HTable hTable){
		Scan scan=new Scan();
		ResultScanner resultScanner = null;
		try {
			resultScanner = hTable.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return resultScanner;
	}
}
