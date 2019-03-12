package com.peng;

import java.util.Random;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.peng.Student.EssentialInformation;
import com.peng.Student.ScoreInformation;

public class TestMain {
	private static final Logger logger=LoggerFactory.getLogger(TestMain.class);
	public static void main(String[] args) throws Exception {
		//创建表
        HBaseHelper.createTable(Student.TABLE_NAME, new String[] { Student.EI_FAMILY_NAME, Student.SI_FAMILY_NAME });
        //定义数据
        String[] names = new String[] { "Li", "Tom", "Jack", "Jackey", "Joy", "Joseph", "Lily", "Sophie", "Ken","Bob" };
        int[] ages = new int[] { 9, 9, 9, 9, 9, 10, 10, 10, 10, 10 };
        String[] genders = new String[] { "boy", "girl", "boy", "girl", "boy", "girl", "boy", "girl", "boy", "girl" };
        String[] classes = new String[] { "1班", "2班", "3班", "1班", "1班", "2班", "2班", "3班", "3班", "3班" };

        //定义随机数 
        Random random = new Random();

        long key = System.currentTimeMillis();
        for (int i = 0; i < 10; i++){
            //循环创建学生
            Student student = new Student("rowkey" + (key + i),
                    new EssentialInformation(names[i], ages[i], genders[i], classes[i]), 
                    new ScoreInformation(random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100)));
            student.save();
        }

        HTable table = new HTable(HBaseHelper.configuration, Student.TABLE_NAME);
        //全表检索
        ResultScanner rs = HBaseHelper.scanALL(table);

        //打印结果
        for (Result r : rs){
            Student student = new Student();
            student.parse(r);
            logger.info(student.toString());
        }
        table.close();


	}

}
