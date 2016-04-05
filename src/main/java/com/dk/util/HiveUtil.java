package com.dk.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveUtil {

	public static Statement createStmt(String driverName, String url,
			String hostName, String hostPassword) throws Exception {
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url, hostName,
				hostPassword);
		Statement stmt = conn.createStatement();
		return stmt;
	}

	public static void createTab(Statement stmt, String tableName, int fdSum,
			String spStr, String dirName) throws Exception {
		// 表中字段
		StringBuffer fileds = new StringBuffer();

		for (int i = 1; i <= fdSum; i++) {
			if (fileds.length() == 0) {
				fileds.append("f" + i + " string");
			} else {
				fileds.append(",").append("f" + i + " string");
			}

		}

		// 创建表
		String sql = "create external table " + tableName + " (" + fileds
				+ ") " + "row format delimited fields terminated by '" + spStr
				+ "'" + " location " + "'" + dirName + "'";
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static long selectTab(Statement stmt, String tableName, String fun,
			int fdNum) throws Exception {
		String sql = "select " + fun + "(f" + fdNum + ") from " + tableName;
		System.out.println(sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			// System.out.println(res.getInt(1));
			return res.getLong(1);
		}
		return 0;
	}

	public static void deleteTab(Statement stmt, String tableName) throws Exception {
		String sql = "drop table if exists " + tableName;
		stmt.execute(sql);		
	}

	public static long selectTab(Statement stmt, String tableName, String fun,
			int fdNum, String compStr, String whereStr) throws Exception {
		String sql = "select " + fun + "(f" + fdNum + ") from " + tableName+" where f"+ fdNum +" "+compStr+" "+whereStr;
		System.out.println(sql);
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			// System.out.println(res.getInt(1));
			return res.getLong(1);
		}
		return 0;
	}

	public static void createTab(Statement stmt, String tableName,
			String srcDirName) throws Exception {
		// create table tableName (f1 string) location srcDirName
		String sql = "create external table " + tableName + " (f string) " + " location " + "'" + srcDirName + "'";
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void formatRec(Statement stmt, String dstDirName, String srcTable,
			String dstTable, String spStr,  int fdSum) throws Exception {
		// create table t4 location "/zxzy" as select f1 from t1 where size(split(f1,","))=24;
		String sql = "create table " + dstTable +" location " + "'" + dstDirName + "' as select f from "+srcTable+" where size(split(f,"  + "'"+spStr+"')) = "+fdSum;
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void toExTable(Statement stmt, String dstTable) throws Exception {
		// ALTER TABLE t4 SET TBLPROPERTIES ('EXTERNAL'='TRUE');
		String sql = "ALTER TABLE "+dstTable+" SET TBLPROPERTIES ('EXTERNAL'='TRUE')";
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void formatField(Statement stmt, String dstDirName,
			String srcTable, String dstTable, String spStr, int fdSum,
			String fdNum, String regExStr) throws Exception {
		String sql="";
		// select f from t1 where not split(f1,",")[1] like "%a%" and not split(f1,",")[2] like "%a%"
		if (fdNum.trim()=="0") {
			sql = "create table " + dstTable +" location " + "'" + dstDirName + "' as select f from "+srcTable+" where not f like '%" + regExStr +"%'";
		}else if (fdNum.trim().split(",").length==1) {
			sql = "create table " + dstTable +" location " + "'" + dstDirName + "' as select f from "+srcTable+" where not split(f,"  + "'"+spStr+"')["+ (Integer.parseInt(fdNum.trim())-1) +"] like '%" + regExStr +"%'";
		}else if (fdNum.trim().split(",").length>1) {
			String str="";
			for (String fd : fdNum.trim().split(",")) {
				if (str=="") {
					str="not split(f,"  + "'"+spStr+"')["+ (Integer.parseInt(fd.trim())-1) +"] like '%" + regExStr +"%'";
				}else {
					str=str+" and not split(f,"  + "'"+spStr+"')["+ (Integer.parseInt(fd.trim())-1) +"] like '%" + regExStr +"%'";
				}
			}
			sql = "create table " + dstTable +" location " + "'" + dstDirName + "' as select f from "+srcTable+" where " + str;
		}
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void selectField(Statement stmt, String dstDirName,
			String srcTable, String dstTable, String spStr, int fdSum,
			String fdNum) throws Exception {
		// create table t4 location "/zxzy" as select f1,f2,f3 from t1;
		
		String str="";
		for (String fd : fdNum.trim().split(",")) {
			if (str=="") {
				str="f"+ fd.trim();
			}else {
				str=str+" ,"+"f"+ fd.trim();
			}
		}
		
		String sql = "create table " + dstTable + " row format delimited fields terminated by '" + spStr
				+ "'"  +" location " + "'" + dstDirName + "' as select "+str+" from "+srcTable;
		System.out.println(sql);
		stmt.execute(sql);
	}

	public static void selectRec(Statement stmt, String dstDirName,
			String srcTable, String dstTable, String spStr, int fdSum,
			String whereStr) throws Exception {
		// TODO Auto-generated method stub
		String sql = "create table " + dstTable + " row format delimited fields terminated by '" + spStr
				+ "'" +" location " + "'" + dstDirName + "' as select * from "+srcTable +" where "+whereStr;
		System.out.println(sql);
		stmt.execute(sql);
	}
}
