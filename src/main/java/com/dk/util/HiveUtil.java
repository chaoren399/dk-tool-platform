package com.dk.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
		String sql = "drop table " + tableName;
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
}
