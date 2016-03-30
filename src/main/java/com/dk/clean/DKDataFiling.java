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
	 * 
	 * @param jarPath: jar包文件的绝对路径
	 * @param spStr
	 * @param fdSum
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
	 * 
	 * @param jarPath
	 * @param spStr
	 * @param regExStr
	 * @param fdNum
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
	 * 
	 * @param jarPath
	 * @param spStr
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
	 * 
	 * @param jarPath
	 * @param spStr
	 * @param compStr: -eq       等于
					   -ne       不等于
                       -gt       大于
	                   -ge       大于等于
	                   -lt       小于
	                   -le       小于等于
                       -In
                       -notin
	 * @param whereStr
	 * @param fdNum
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
