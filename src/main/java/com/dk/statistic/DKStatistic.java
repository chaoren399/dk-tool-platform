package com.dk.statistic;

import java.sql.Statement;

import com.dk.util.HiveUtil;

public class DKStatistic {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	/**
	 * 
	 * @param fun: avg,min,max,sum
	 * @param fdSum
	 * @param spStr
	 * @param fdNum
	 * @param dirName
	 * @param hostIp
	 * @param hostPort: 10000
	 * @param hostName
	 * @param hostPassword
	 * @return
	 */
	public static long  count(String fun, int fdSum, String spStr, int fdNum,
			String dirName, String hostIp, String hostPort, String hostName, String hostPassword) {
		
		// 临时表名
		String tableName = "HiveTmpTabl_" + System.currentTimeMillis();
		Statement stmt = null;
		String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default";
		long data = 0;
		
		try {
			stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
			HiveUtil.createTab(stmt, tableName, fdSum, spStr, dirName);
			data = HiveUtil.selectTab(stmt, tableName, fun, fdNum);
			HiveUtil.deleteTab(stmt, tableName);

		} catch (Exception e) {
			e.printStackTrace();
		}

		//return "the " + fun + " for field_" + fdNum + " is " + data;
		return data;

	}
/**
 * 
 * @param fun:count
 * @param fdSum
 * @param spStr
 * @param fdNum
 * @param compStr: >, <, >=, <=, =,!=
 * @param whereStr
 * @param dirName
 * @param hostIp
 * @param hostPort: 10000
 * @param hostName
 * @param hostPassword
 * @return
 */
	//count( fun,  fdSum,  spStr,  fdNum, compStr, whereStr,	 dirName,  hostIp,  hostName,  hostPassword)
	public static long count(String fun, int fdSum, String spStr, int fdNum,
			String compStr, String whereStr, String dirName, String hostIp, String hostPort,
			String hostName, String hostPassword) {
		// 临时表名
				String tableName = "HiveTmpTabl_" + System.currentTimeMillis();
				Statement stmt = null;
				String url = "jdbc:hive2://" + hostIp + ":"+hostPort+"/default";
				long data = 0;
				
				try {
					stmt = HiveUtil.createStmt(driverName, url, hostName, hostPassword);
					HiveUtil.createTab(stmt, tableName, fdSum, spStr, dirName);
					data = HiveUtil.selectTab(stmt, tableName, fun, fdNum,compStr,whereStr);
					HiveUtil.deleteTab(stmt, tableName);

				} catch (Exception e) {
					e.printStackTrace();
				}

				//return "the " + fun + " for field_" + fdNum + " is " + data;
				return data;
	}

}
