package org.apache.pig.backend.hadoop.executionengine.spark;

import java.net.Socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.Serializable;

/* TCPClient to receive Objects from TCPServer */

public class BroadCastClient implements Serializable{

	private String host;
	private int port;
	
	private Socket client;
	Object response=null;
	OutputStream outToServer;
	DataOutputStream out;
	ObjectInputStream inFromServer;
	
	public BroadCastClient(String host, int port){

		this.host = host;
		this.port = port;

	}

	public Object getBroadCastMessage(String request){

		
		try{
			
			System.out.println("Connecting to " + host
					+ " on port " + port);
			client = new Socket(host, port);
			
			outToServer = client.getOutputStream();
			out = new DataOutputStream(outToServer);

			out.writeUTF(request);
			inFromServer = new ObjectInputStream(client.getInputStream());
			
			response = inFromServer.readObject();
			System.out.println("Server says " + response);
			
			outToServer.close();
			out.close();
			client.close();			
			
		}catch(Exception e){
			e.printStackTrace();
		}

		return response;
	}
}
