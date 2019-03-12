package com.peng;

import java.util.NavigableMap;

public abstract class FamilyData {
	protected String familyName;
	protected abstract String[] getColumnNames();
	protected abstract String[] getValues();
	protected abstract void parse(NavigableMap<byte[], byte[]> kv);

	public ColumnFamilyValue toFamilyValue(){
		ColumnFamilyValue familyValue=new ColumnFamilyValue();
		familyValue.familyName=familyName;
		familyValue.columnName=getColumnNames();
		familyValue.values=getValues();
		return familyValue;
	}
}
