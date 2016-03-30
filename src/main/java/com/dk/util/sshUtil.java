package com.dk.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

public class sshUtil {
	public static void scp(String hostIp, String hostName,
			String hostPassword, String localFile, String remotePath) throws Exception {
		Connection conn=createConn(hostIp, hostName,hostPassword);
		
		SCPClient scpClient = new SCPClient(conn);
		scpClient.put(localFile, remotePath,"0777");
		
		System.out.println("upLoad success");
	}
	
	static Connection createConn(String hostIp, String hostName,
			String hostPassword) throws Exception{
		Connection conn = new Connection(hostIp);
		conn.connect();
		boolean isAuthenticated = conn.authenticateWithPassword(hostName, hostPassword);

		if (isAuthenticated == false)
			throw new IOException("Authentication failed.");
		return conn;
	}


	public static void exe(String cmd, String hostIp, String hostName,
			String hostPassword) throws Exception {
		Connection conn=createConn(hostIp, hostName,hostPassword);
		Session sess = conn.openSession();
		sess.requestPTY("vt100", 80, 24, 640, 480, null);
		sess.execCommand(cmd);
		
		InputStream stdout = new StreamGobbler(sess.getStdout());  
 
        BufferedReader stdoutReader = new BufferedReader(  
                new InputStreamReader(stdout));  

        System.out.println("Here is the output from stdout:");  
        while (true) {  
            String line = stdoutReader.readLine();  
            if (line == null)  
                break;  
            System.out.println(line);  
        }  

		//System.out.println("ExitCode: " + sess.getExitStatus());

		sess.close();

		conn.close();
		
	}
}
