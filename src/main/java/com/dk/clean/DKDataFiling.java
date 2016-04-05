package com.dk.clean;

import java.sql.Statement;

import com.dk.util.HiveUtil;

public class DKDataFiling {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void formatRec(String spStr, int fdSum, String srcDirName,
			String dstDirName, String hostIp, String hostPort, String hostName,
			String hostPassword) throws Exception {
		String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
		Statement stmt = null;
		String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";
		stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
		HiveUtil.createTab(stmt, srcTable, srcDirName);

		String dstTable = srcTable + "_dst";
		HiveUtil.formatRec(stmt, dstDirName, srcTable, dstTable, spStr, fdSum);

		HiveUtil.toExTable(stmt, dstTable);
		HiveUtil.deleteTab(stmt, srcTable);
		HiveUtil.deleteTab(stmt, dstTable);
	}
	
	//formatField (String regExStr,int fdNum,String srcDirName,String dstDirName,,String threadNum,String hostIp,String hostName,String hostPassword)
	public static void formatField(String spStr, int fdSum,String fdNum, String regExStr, String srcDirName,
			String dstDirName, String hostIp, String hostPort, String hostName,
			String hostPassword) throws Exception {
		String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
		Statement stmt = null;
		String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";
		stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
		HiveUtil.createTab(stmt, srcTable, srcDirName);

		String dstTable = srcTable + "_dst";
		HiveUtil.formatField(stmt, dstDirName, srcTable, dstTable, spStr, fdSum,fdNum,regExStr);
		
		HiveUtil.toExTable(stmt, dstTable);
		HiveUtil.deleteTab(stmt, srcTable);
		HiveUtil.deleteTab(stmt, dstTable);
	}

	//String selectField (int[] fdAr,String srcDirName,String dstDirName,String threadNum,String hostIp,String hostName,String hostPassword)
	public static void selectField(String spStr, int fdSum,String fdNum, String srcDirName,
			String dstDirName, String hostIp, String hostPort, String hostName,
			String hostPassword) throws Exception {
		String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
		Statement stmt = null;
		String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";
		stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
		HiveUtil.createTab(stmt, srcTable, fdSum, spStr, srcDirName);

		String dstTable = srcTable + "_dst";
		HiveUtil.selectField(stmt, dstDirName, srcTable, dstTable, spStr, fdSum,fdNum);
		
		HiveUtil.toExTable(stmt, dstTable);
		HiveUtil.deleteTab(stmt, srcTable);
		HiveUtil.deleteTab(stmt, dstTable);
	}
	
	
	//String selectRec (Stirng whereStr,int fdNum,String srcDirName,String dstDirName,String threadNum,String hostIp,String hostName,String hostPassword)
	public static void selectRec(String spStr, int fdSum, String whereStr, String srcDirName,
			String dstDirName, String hostIp, String hostPort, String hostName,
			String hostPassword) throws Exception {
		String srcTable = "HiveTmpTabl_" + System.currentTimeMillis();
		Statement stmt = null;
		String url = "jdbc:hive2://" + hostIp + ":" + hostPort + "/default";
		stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
		HiveUtil.createTab(stmt, srcTable, fdSum, spStr, srcDirName);

		String dstTable = srcTable + "_dst";
		HiveUtil.selectRec(stmt, dstDirName, srcTable, dstTable, spStr, fdSum,whereStr);
		
		HiveUtil.toExTable(stmt, dstTable);
		HiveUtil.deleteTab(stmt, srcTable);
		HiveUtil.deleteTab(stmt, dstTable);
	}
	
	public static void main(String[] args) throws Exception {
		String hostIp = "192.168.50.102";
		String hostName = "root";
		String hostPassword = "123456";
		int fdSum = 24;
		String spStr = ",";
		String srcDirName = "/zxh/class_build_in";
		String hostPort = "10000";
		//formatRec(spStr, fdSum, srcDirName, "/zxh/formatRec", hostIp, hostPort,hostName, hostPassword);
//		formatField(spStr, fdSum, "1, 2, 3", "0", srcDirName, "/zxh/formatFieldout2", hostIp, hostPort, hostName, hostPassword);
		selectField(spStr, fdSum, "1,2,3", srcDirName, "/zxh/selectFieldout2", hostIp, hostPort, hostName, hostPassword);
//		selectRec(spStr, fdSum, "f1 = 1", srcDirName, "/zxh/selectRecout", hostIp, hostPort, hostName, hostPassword);
	}
}
