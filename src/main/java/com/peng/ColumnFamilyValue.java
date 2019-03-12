package com.peng;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
* @ClassName: ColumnFamilyValue 
* @Description: 一个ColumnFamily结构的对象 包括了family的名称 列数组 值数组
* @author pengcq 412940226@qq.com 
* @date 2017年9月23日 下午5:03:46 
*
 */
public class ColumnFamilyValue {
	private static final Logger logger=LoggerFactory.getLogger(ColumnFamilyValue.class);
	public String familyName;
	public String[] columnName;
	public String[] values;
	
	public void put(Put put) throws Exception{
		if(columnName.length!=values.length){
			throw new Exception("columnFamily is " + familyName + ",columns's length no equals datas's length!");
		}else {
			for (int i = 0; i < columnName.length; i++) {
				put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName[i]), Bytes.toBytes(values[i]));
				logger.info("columnFamily put , familyName="+familyName+","+columnName[i]+","+values[i]);
			}
		}
	}
}
