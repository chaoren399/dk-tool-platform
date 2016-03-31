package com.dk.clean;

import com.dk.util.sshUtil;

public class DKDataFiling {
	/**
	 * 上传jar包
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param localFile：本地jar包绝对路径
	 * @param remotePath：服务器文件夹路径
	 */
	public static void upLoadJar(String hostIp, String hostName, String hostPassword,
			String localFile, String remotePath) {
		try {
			sshUtil.scp(hostIp, hostName, hostPassword, localFile, remotePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 不符合该数量的记录将被清除
	 * @param jarPath: jar包文件的绝对路径
	 * @param spStr：分隔符
	 * @param fdSum：字段数量
	 * @param srcDirName
	 * @param dstDirName
	 * @param threadNum
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 */
	public static void formatRec(String jarPath, String spStr, int fdSum,
			String srcDirName, String dstDirName, String threadNum,
			String hostIp, String hostName, String hostPassword) {
		
		// hadoop jar xxx.jar [mainclass] spStr fdSum srcDirName dstDirName threadNum
		
		String cmd = "hadoop jar " + jarPath
				+ " com.dk.formatRec.FormatRecDriver " + spStr + " " + fdSum
				+ " " + srcDirName + " " + dstDirName + " " + threadNum;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 符合该表达式字段的记录将被剔除，检查哪个字段是否符合正则，0为全部检查
	 * @param jarPath
	 * @param spStr：分隔符
	 * @param regExStr：正则表达式
	 * @param fdNum: ：字段编号，0为全部检查
	 * @param srcDirName
	 * @param dstDirName
	 * @param threadNum
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 */
	public static void formatField(String jarPath, String spStr, String regExStr,int fdNum,
			String srcDirName, String dstDirName, String threadNum,
			String hostIp, String hostName, String hostPassword) {
		//  hadoop jar xxx.jar [mainclass] spStr regExStr fdNum srcDirName dstDirName threadNum
		
		String cmd = "hadoop jar " + jarPath
				+ " com.dk.formatField.FormatFieldDriver " + spStr+ " " + regExStr + " " + fdNum
				+ " " + srcDirName + " " + dstDirName + " " + threadNum;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 整数数组，内容是要保留的字段序号，没有编号的字段将去除
	 * @param jarPath
	 * @param spStr：分隔符
	 * @param fdAr：字段编号，eg：1,2,3
	 * @param srcDirName
	 * @param dstDirName
	 * @param threadNum
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 */
	public static void selectField(String jarPath, String spStr, String fdAr,
			String srcDirName, String dstDirName, String threadNum,
			String hostIp, String hostName, String hostPassword) {
		//  hadoop jar xxx.jar [mainclass] spStr fdAr srcDirName dstDirName threadNum
		
		String cmd = "hadoop jar " + jarPath
				+ " com.dk.selectField.SelectFieldDriver " + spStr+ " " + fdAr
				+ " " + srcDirName + " " + dstDirName + " " + threadNum;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 符合筛选条件的将被留下
	 * @param jarPath
	 * @param spStr
	 * @param compStr: -eq       等于
					   -ne       不等于
                       -gt       大于
	                   -ge       大于等于
	                   -lt       小于
	                   -le       小于等于
                       -in
                       -notin
	 * @param whereStr：条件
	 * @param fdNum：字段编号
	 * @param srcDirName
	 * @param dstDirName
	 * @param threadNum
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 */
	public static void selectRec(String jarPath, String spStr, String compStr, String whereStr, int fdNum,
			String srcDirName, String dstDirName, String threadNum,
			String hostIp, String hostName, String hostPassword) {
		//  hadoop jar xxx.jar [mainclass] spStr compStr whereStr fdNum srcDirName dstDirName threadNum
		
		String cmd = "hadoop jar " + jarPath
				+ " com.dk.selectRec.SelectRecDriver " + spStr+ " " + compStr + " "+ whereStr+ " " + fdNum
				+ " " + srcDirName + " " + dstDirName + " " + threadNum;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
