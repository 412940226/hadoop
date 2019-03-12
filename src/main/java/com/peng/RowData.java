package com.peng;

import org.apache.hadoop.hbase.client.Result;

public interface RowData {
	/**
	 * 将对象保存到HBase中
	 * @throws Exception
	 */
	public void save() throws Exception;
	/**
	 * 解析从HBase读取的数据
	 * @param result
	 * @throws Exception
	 */
	public void parse(Result result) throws Exception;
}
