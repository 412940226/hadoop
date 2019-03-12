package com.peng;

import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Student implements RowData {
	private static final Logger logger=LoggerFactory.getLogger(Student.class);
	public static final String EI_FAMILY_NAME = "essentialInformation";
	public static final String SI_FAMILY_NAME = "scoreInformation";

	public void save() throws Exception {
		ColumnFamilyValue[] columnFamily = new ColumnFamilyValue[] { essentialInformation.toFamilyValue(),
                scoreInformation.toFamilyValue() };
        HBaseHelper.addRow(TABLE_NAME, rowKey, columnFamily);

	}

	public void parse(Result rs) throws Exception {
		this.rowKey=new String(rs.getRow());
        NavigableMap<byte[], byte[]> ei_map = rs.getFamilyMap(Bytes.toBytes(EI_FAMILY_NAME));       
        NavigableMap<byte[], byte[]> si_map = rs.getFamilyMap(Bytes.toBytes(SI_FAMILY_NAME));

        this.essentialInformation=new EssentialInformation();       
        this.essentialInformation.parse(ei_map);

        this.scoreInformation=new ScoreInformation();
        this.scoreInformation.parse(si_map);


	}
	public EssentialInformation essentialInformation;
	public ScoreInformation scoreInformation;
	public static final String TABLE_NAME = "student";
	public String rowKey="";
	
	public Student() {
		
	}

	public Student(String rowKey, EssentialInformation essentialInformation, ScoreInformation scoreInformation) {
		super();
		this.essentialInformation = essentialInformation;
		this.scoreInformation = scoreInformation;
		this.rowKey = rowKey;
	}

	@Override
	public String toString() {
		StringBuilder builder=new StringBuilder();
        builder.append("rowkey="+this.rowKey);
        builder.append(",");
        builder.append(this.essentialInformation.toString());
        builder.append(this.scoreInformation.toString());
        builder.append("\n");
        return builder.toString();

	}
	
	public static class EssentialInformation extends FamilyData {
		public String name;
		public int age;
		public String gender;
		public String classesName;
		private static final String[] columnsName = new String[] { "name", "age", "gender", "classesName" };
		public EssentialInformation() {
			
		}
		
		public EssentialInformation(String name, int age, String gender, String classesName) {
			super();
			this.name = name;
			this.age = age;
			this.gender = gender;
			this.classesName = classesName;
			
			this.familyName= EI_FAMILY_NAME;
		}

		@Override
		protected String[] getColumnNames() {
			return columnsName;
		}

		@Override
		protected String[] getValues() {
			return new String[]{name, String.valueOf(age), gender,classesName};
		}

		@Override
		protected void parse(NavigableMap<byte[], byte[]> kv) {
			this.name=new String(kv.get(Bytes.toBytes("name")));
			this.age=Integer.parseInt(new String(kv.get(Bytes.toBytes("age"))));
			this.gender=new String(kv.get(Bytes.toBytes("gender")));
			this.classesName=new String(kv.get(Bytes.toBytes("classesName")));
		}

		@Override
		public String toString() {
			 StringBuilder builder=new StringBuilder();
	         builder.append("{name="+name);
	         builder.append(",");
	         builder.append("age="+age);
	         builder.append(",");
	         builder.append("gender="+gender);
	         builder.append(",");
	         builder.append("classesName="+classesName);
	         builder.append("},");
	         return builder.toString();

		}

	}
	public static class ScoreInformation extends FamilyData {
		public int math;
		public int english;
		public int chinese;
		public int sports;
		private static final String[] columnNames = new String[] { "math", "english", "chinese", "sports" };
		
		
		public ScoreInformation() {
			
		}

		public ScoreInformation(int math, int english, int chinese, int sports) {
			super();
			this.math = math;
			this.english = english;
			this.chinese = chinese;
			this.sports = sports;
			
			this.familyName=SI_FAMILY_NAME;
		}

		@Override
		protected String[] getColumnNames() {
			return columnNames;
		}

		@Override
		protected String[] getValues() {
			return new String[]{String.valueOf(math), String.valueOf(english), String.valueOf(chinese),String.valueOf(sports)};
		}

		@Override
		protected void parse(NavigableMap<byte[], byte[]> kv) {
			this.math=Integer.parseInt(new String(kv.get(Bytes.toBytes("math"))));
			this.english=Integer.parseInt(new String(kv.get(Bytes.toBytes("english"))));
			this.chinese=Integer.parseInt(new String(kv.get(Bytes.toBytes("chinese"))));
			this.sports=Integer.parseInt(new String(kv.get(Bytes.toBytes("sports"))));
		}

		@Override
		public String toString() {
			StringBuilder builder=new StringBuilder();
            builder.append("{math="+math);
            builder.append(",");
            builder.append("english="+english);
            builder.append(",");
            builder.append("chinese="+chinese);
            builder.append(",");
            builder.append("sports="+sports);
            builder.append("},");
            return builder.toString();
		}

	}
	
}
