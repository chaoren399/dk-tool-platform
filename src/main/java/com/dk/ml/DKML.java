package com.dk.ml;

import com.dk.util.sshUtil;

public class DKML {
	
	/**
	 * 上传jar包
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param localFile
	 * @param remotePath
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
	 * 训练推荐模型
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param rank ：模型潜在因素个数，默认10
	 * @param numIterations：迭代次数，默认10
	 */
	public static void ALSModelBuild(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,int rank,int numIterations) {
		String cmd = "spark-submit --class com.dk.als.ALSModelBuild --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + rank+ " " + numIterations;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 给商品推荐用户
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath：数据每行一条
	 * @param modelPath
	 * @param outputPath：数据格式 4--1:4.9961740014799565,1:4.9961740014799565,1:1.0016426994325465,1:1.0016426994325465
	 */
	public static void recommendUser(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String outputPath) {
		String cmd = "spark-submit --class com.dk.als.RMUsers --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + outputPath;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 给用户推荐商品
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param outputPath
	 */
	public static void recommendProduct(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String outputPath) {
		String cmd = "spark-submit --class com.dk.als.RMProducts --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + outputPath;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 挖掘关联规则的频繁项集
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath：数据以逗号分割
	 * @param outputPath：数据格式 [t,x]: 3
	 * @param minSupport：最小支持度，默认0.3
	 */
	public static void FPGrowthModelBuild(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String outputPath,double minSupport) {
		String cmd = "spark-submit --class com.dk.fpgrowth.FPGrowthModel --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + outputPath+ " " + minSupport;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 构建逻辑回归模型
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param dataType：数据类型，LibSVM、LabeledPoints
	 * @param numClass：分类数目
	 */
	public static void LRModelBuild(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String dataType,int numClass) {
		String cmd = "spark-submit --class com.dk.lr.LRModelBuild --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + dataType+ " " + numClass;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 用逻辑回归进行预测
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param outputPath: 数据格式 (692,[129,130,···],[···,94.0,59.0])--0.0
	 * @param dataType：数据类型，LibSVM、LabeledPoints
	 */
	public static void LRModelPredict(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String outputPath,String dataType) {
		String cmd = "spark-submit --class com.dk.lr.LRModelPredict --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + outputPath+ " " + dataType;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 构建朴素贝叶斯模型
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param dataType： 数据类型，LibSVM、LabeledPoints
	 */
	public static void NBModelBuild(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String dataType) {
		String cmd = "spark-submit --class com.dk.nb.NBModelBuild --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + dataType;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 朴素贝叶斯模型预测
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param outputPath: 数据格式 (692,[129,130,···],[···,94.0,59.0])--0.0
	 * @param dataType：数据类型，LibSVM、LabeledPoints
	 */
	public static void NBModelPredict(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String outputPath,String dataType) {
		String cmd = "spark-submit --class com.dk.nb.NBModelPredict --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + outputPath+ " " + dataType;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 主成分分析
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param outputPath
	 * @param dataType：数据类型，LibSVM、LabeledPoints
	 * @param k：主成分数目
	 */
	public static void PCAModel(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String outputPath,String dataType,int k) {
		String cmd = "spark-submit --class com.dk.pca.PCAModel --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + outputPath+ " " + dataType+ " " + k;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 随机森林分类模型
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param dataType：数据类型，LibSVM、LabeledPoints
	 * @param numClass：分类数目，2或者更多
	 */
	public static void RFClassModelBuild(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String dataType,int numClass) {
		String cmd = "spark-submit --class com.dk.randomforest.RFClassModelBuild --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + dataType+ " " + numClass;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 随机森林回归模型
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param dataType
	 */
	public static void RFRegresModelBuild(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String dataType) {
		String cmd = "spark-submit --class com.dk.randomforest.RFRegresModelBuild --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + dataType;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 随机森林模型预测
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param outputPath
	 * @param dataType
	 */
	public static void RFModelPredict(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String outputPath,String dataType) {
		String cmd = "spark-submit --class com.dk.randomforest.RFModelPredict --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + outputPath+ " " + dataType;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 支持向量机模型构建，仅支持二分类
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param dataType
	 */
	public static void SVMModelBuild(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String dataType) {
		String cmd = "spark-submit --class com.dk.svm.SVMModelBuild --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + dataType;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 支持向量机模型预测
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param outputPath
	 * @param dataType
	 */
	public static void SVMModelPredict(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String outputPath,String dataType) {
		String cmd = "spark-submit --class com.dk.svm.SVMModelPredict --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + outputPath+ " " + dataType;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 构建聚类模型
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param dataType
	 * @param numClusters
	 */
	public static void KMModelBuild(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String dataType,int numClusters) {
		String cmd = "spark-submit --class com.dk.kmeans.KMModelBuild --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + dataType+ " " + numClusters;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	/**
	 * 聚类模型预测
	 * @param hostIp
	 * @param hostName
	 * @param hostPassword
	 * @param jarPath
	 * @param masterUrl
	 * @param inputPath
	 * @param modelPath
	 * @param outputPath
	 * @param dataType
	 */
	public static void KMModelPredict(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String outputPath,String dataType) {
		String cmd = "spark-submit --class com.dk.kmeans.KMModelPredict --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + outputPath+ " " + dataType;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	public static void GMModelBuild(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String dataType,int numClusters) {
		String cmd = "spark-submit --class com.dk.gaussian.GMModelBuild --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + dataType+ " " + numClusters;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
	
	public static void GMModelPredict(String hostIp, String hostName,
			String hostPassword, String jarPath, String masterUrl,
			 String inputPath,String modelPath,String outputPath,String dataType) {
		String cmd = "spark-submit --class com.dk.gaussian.GMModelPredict --master "
				+ masterUrl + " " + jarPath + " " + inputPath+ " " + modelPath+ " " + outputPath+ " " + dataType;
		System.out.println(cmd);
		try {
			sshUtil.exe(cmd, hostIp, hostName, hostPassword);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}
}
