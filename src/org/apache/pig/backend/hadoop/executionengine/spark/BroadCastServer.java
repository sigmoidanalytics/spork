package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.Iterator;

/* TCPServer thread to serve missed Objects */

public class BroadCastServer extends Thread{

	private ServerSocket serverSocket;
	private static Map<Object, Object> storage = null;

	private static Stack<Object> stack;

	int port;

	public BroadCastServer(){}

	public BroadCastServer(int port) throws IOException{

		this.port = port;	
		serverSocket = new ServerSocket(port);

	}

	public void run(){
		while(true){
			try{

				System.out.println("Waiting for client on port " +
						serverSocket.getLocalPort() + "...");

				Socket server = serverSocket.accept();
				System.out.println("Just connected to "
						+ server.getRemoteSocketAddress());
				DataInputStream in =
						new DataInputStream(server.getInputStream());
				String request = in.readUTF();
				System.out.println("Executor asking for :" + request);
				ObjectOutputStream out =
						new ObjectOutputStream(server.getOutputStream());

				
				if(request.equalsIgnoreCase("json_schema")){
					
					try{
						Object schema = stack.pop();
						System.out.println("Sending Json_Schema :" + schema);							
						out.writeObject(schema);
					}catch(Exception e){ 
						//e.printStackTrace();
						out.writeObject(storage.get(request));
					}
					
				}else{
					
					if(storage.get(request) != null){
						out.writeObject(storage.get(request));						

					}else{

						out.writeObject(null);
					}
				}
				
				server.close();
				in.close();
				out.close();

			}catch(Exception s){				
				s.printStackTrace();
				break;
			}
		}
	}

	public void startBroadcastServer(int port){

		try{	

			storage = new HashMap<Object, Object>();
			stack = new Stack<Object>();

			Thread t = new BroadCastServer(port);
			t.start();

		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public void addResource(String reference, Object resource){

		storage.put(reference, resource);

	}

	public void addResource(String schema, String reference, Object resource){

		if(schema.equalsIgnoreCase("json_schema")){
			
			Stack<Object> tmp_stack = stack;
			
			Iterator<Object> iterator = tmp_stack.iterator();
			boolean found = false;
			
			while(iterator.hasNext()){
				if(iterator.next().equals(resource)){
					found = true;
				}
			}
			if(!found){
				stack.push((Object) resource);
				storage.put(reference, resource);
			}
			
		}

	}

}
