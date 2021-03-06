package com.dk.clean;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import com.dk.util.HiveUtil;

public class DKDataFiling {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void formatRec(String spStr, int fdSum, String srcDirName,
			String dstDirName, String hostIp, String hostPort, String hostName,
			String hostPassword) throws Exception {

		String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
		String dstTable = srcTable + "_dst";

		String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url, hostName,hostPassword);
		Statement stmt = conn.createStatement();
		
		HiveUtil.createTab(stmt, srcTable, srcDirName);

		try {
			HiveUtil.formatRec(stmt, dstDirName, srcTable, dstTable, spStr,
					fdSum);
			HiveUtil.toExTable(stmt, dstTable);
		} finally {

			HiveUtil.deleteTab(stmt, srcTable);
			HiveUtil.deleteTab(stmt, dstTable);
			stmt.close();
			conn.close();
		}
		
	}

	// formatField (String regExStr,int fdNum,String srcDirName,String
	// dstDirName,,String threadNum,String hostIp,String hostName,String
	// hostPassword)
	public static void formatField(String spStr, int fdSum, String fdNum,
			String regExStr, String srcDirName, String dstDirName,
			String hostIp, String hostPort, String hostName, String hostPassword)
			throws Exception {

		String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
		String dstTable = srcTable + "_dst";

		String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url, hostName,hostPassword);
		Statement stmt = conn.createStatement();
		
		stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
		HiveUtil.createTab(stmt, srcTable, srcDirName);

		try {
			HiveUtil.formatField(stmt, dstDirName, srcTable, dstTable, spStr,
					fdSum, fdNum, regExStr);
			HiveUtil.toExTable(stmt, dstTable);
		} finally {
			HiveUtil.deleteTab(stmt, srcTable);
			HiveUtil.deleteTab(stmt, dstTable);
			stmt.close();
			conn.close();
		}
	}

	// String selectField (int[] fdAr,String srcDirName,String dstDirName,String
	// threadNum,String hostIp,String hostName,String hostPassword)
	public static void selectField(String spStr, int fdSum, String fdNum,
			String srcDirName, String dstDirName, String hostIp,
			String hostPort, String hostName, String hostPassword)
			throws Exception {
		String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
		String dstTable = srcTable + "_dst";

		String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url, hostName,hostPassword);
		Statement stmt = conn.createStatement();
		stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
		HiveUtil.createTab(stmt, srcTable, fdSum, spStr, srcDirName);

		try {
			HiveUtil.selectField(stmt, dstDirName, srcTable, dstTable, spStr,
					fdSum, fdNum);

			HiveUtil.toExTable(stmt, dstTable);
		} finally {
			HiveUtil.deleteTab(stmt, srcTable);
			HiveUtil.deleteTab(stmt, dstTable);
			stmt.close();
			conn.close();
		}

	}

	// String selectRec (Stirng whereStr,int fdNum,String srcDirName,String
	// dstDirName,String threadNum,String hostIp,String hostName,String
	// hostPassword)
	public static void selectRec(String spStr, int fdSum, String whereStr,
			String srcDirName, String dstDirName, String hostIp,
			String hostPort, String hostName, String hostPassword)
			throws Exception {
		String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
		String dstTable = srcTable + "_dst";

		String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url, hostName,hostPassword);
		Statement stmt = conn.createStatement();
		
		stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
		HiveUtil.createTab(stmt, srcTable, fdSum, spStr, srcDirName);
		try {
			HiveUtil.selectRec(stmt, dstDirName, srcTable, dstTable, spStr,
					fdSum, whereStr);

			HiveUtil.toExTable(stmt, dstTable);
		} finally {
			HiveUtil.deleteTab(stmt, srcTable);
			HiveUtil.deleteTab(stmt, dstTable);
			stmt.close();
			conn.close();
		}

	}

	public static void main(String[] args) throws Exception {
		String hostIp = "192.168.50.102";
		String hostName = "root";
		String hostPassword = "123456";
		int fdSum = 20;
		String spStr = ",";
		String srcDirName = "/zzy";
		String hostPort = "10000";
//		formatRec(spStr, fdSum, srcDirName, "/zxh/formatRec2", hostIp,hostPort,hostName, hostPassword);
//		formatField(spStr, fdSum, "1,1,1", "a,b,c", srcDirName,"/zxh/formatFieldout2", hostIp, hostPort, hostName, hostPassword);
//		selectField(spStr, fdSum, "1,2,3", srcDirName, "/zxh/selectFieldout2",
//				hostIp, hostPort, hostName, hostPassword);
		 selectRec(spStr, fdSum, "f1 = 1", srcDirName, "/zxh/selectRecout",
		 hostIp, hostPort, hostName, hostPassword);
	}
}
