

import java.io.*;

import java.net.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Vector;
import java.util.stream.Collectors;


import helma.xmlrpc.WebServer;
import helma.xmlrpc.XmlRpcClient;
import helma.xmlrpc.XmlRpcException;
import org.apache.log4j.Logger;

//Client class 
public class Reducer {
	String filePath;
	String filePathOutput;
	String server_url;
	 static Logger log = Logger.getLogger(Reducer.class.getName());
	public static void main (String [] args) {
		try {
			log.debug("Reducer VM started");
			// Invoke me as <http://localhost:8080/RPC2>.
			WebServer server = new WebServer(8050);
			server.addHandler("sample", new Reducer());

		} catch (Exception exception) {
			System.err.println("JavaServer: " + exception.toString());
		}
	}

	public void reducerMain(String ip) throws IOException {
		
		log.debug("Reducer process started with IP"+ip);
		try {
		
			BufferedReader fs = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/MapReduceConfig.properties")));

			Properties props = new Properties();
			props.load(fs);

			String KeyValueIP = props.getProperty("KeyValueIP");

			server_url = "http://"+KeyValueIP+":3389";
			
			log.debug("Reducer connected to KeyValueStore on IP"+KeyValueIP);
			
			
			int port = Integer.parseInt(props.getProperty("MapReducePort"));
			log.debug("Reducer connected to Master on IP"+ip);
			Socket s = new Socket(ip, port);

			DataInputStream dis = new DataInputStream(s.getInputStream());
			DataOutputStream dos = new DataOutputStream(s.getOutputStream());

			filePath = props.getProperty("IntermediateInputFolder");
			filePathOutput = props.getProperty("ReducerOutPutFolder");

			String received = dis.readUTF();
			int code = Integer.parseInt(received.substring(received.length()-1));

			received = received.substring(0, received.length()-1);
			String status=null;
			if(code == 0) {
				status = writeDataToFile(received);
			}else if(code == 1) {
				status = writeDataToFileInvertedIndex(received);
			}
			if(status.equals("Done"))
				dos.writeUTF("Done");
			else
				dos.writeUTF("Not Done");
			log.debug("Reducer process done with IP"+ip);
			s.close();

			dis.close();
			dos.close();
		} catch (EOFException e) {
			System.out.println("Waiting for data");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String writeDataToFileInvertedIndex(String received) throws IOException {
		// TODO Auto-generated method stub

		String[] fileStringArray = received.split("#");
		String fileName =fileStringArray[0];
		int reducerNum = Integer.parseInt(fileStringArray[1]);
		//File file = new File(filePath+"\\"+fileName);
		//	BufferedReader br = null;
		System.out.println(fileName+"filename in reducer"+filePath);
		//br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(filePath+"\\"+fileName)));
		//br = new BufferedReader(new FileReader(filePath+"\\"+fileName));

		//br = new BufferedReader(new FileReader(filePath+"\\"+fileName));

		String result=null;

		try {
			XmlRpcClient server = new XmlRpcClient(server_url);

			// Build our parameter list.
			Vector params = new Vector();
			params.addElement(new String(filePath));
			params.addElement(new String(fileName));


			// Call the server, and get our result.
			result =(String) server.execute("sample.getValueForInter", params);

		} catch (XmlRpcException exception) {
			System.err.println("JavaClient: XML-RPC Fault #" +
					Integer.toString(exception.code) + ": " +
					exception.toString());
		} catch (Exception exception) {
			System.err.println("JavaClient: " + exception.toString());
		}

		String data= new String();
		Scanner scanner = new Scanner(result);
		Map<String, String> map = new LinkedHashMap<String, String>();

		//try {
		//			while((data=br.readLine())!=null) {
		//				//System.out.println(data+"data");
		//				String[] datainput = data.split(",");
		while(scanner.hasNextLine()) {

			String[] datainput = scanner.nextLine().split(",");
			if(datainput.length==1)
				continue;
			String word = datainput[0];
			String fileList = datainput[1];
			if ( word!=null) {
				if(map.containsKey(word)) {
					if(!map.get(word).contains(fileList))
						map.put(word, map.get(word)+fileList);
				}else {
					map.put(word, fileList);
				}

			}
		}
		//}

		//		catch (IOException e) {
		//			System.out.println(e);
		//		} 
		//fileName = "InvertedIndexReducer"+fileName;
		//		String redFile = filePathOutput+"\\"+fileName;
		//		File reduce = new File(redFile);
		//		BufferedWriter bw = new BufferedWriter(new FileWriter(reduce, true));
		StringBuilder sb = new StringBuilder();
		map.forEach((k, v) -> {
			//	try {
			//	bw.write(k + "," + v+"\n");
			sb.append(k + "," + v+"\n");
			//			} catch (IOException e) {
			//				System.out.println("Couldn't write to file ");
			//			}
		});

		//bw.close();

		try {
			XmlRpcClient server = new XmlRpcClient(server_url);

			// Build our parameter list.
			Vector params = new Vector();
			params.addElement(new String(filePathOutput));
			params.addElement(new String(fileName));
			params.addElement(new String(sb.toString()));


			// Call the server, and get our result.
			result =(String) server.execute("sample.writeDataToFile", params);

		} catch (XmlRpcException exception) {
			System.err.println("JavaClient: XML-RPC Fault #" +
					Integer.toString(exception.code) + ": " +
					exception.toString());
		} catch (Exception exception) {
			System.err.println("JavaClient: " + exception.toString());
		}


		return "Done";


	}

	public String writeDataToFile(String fileString) throws IOException {

		String[] fileStringArray = fileString.split("#");
		String fileName =fileStringArray[0];
		int reducerNum = Integer.parseInt(fileStringArray[1]);
		File file = new File( filePath+"\\"+fileName);

		//		BufferedReader br = null;
		//		br = new BufferedReader(new FileReader(filePath+"\\"+fileName));
		String result=null;
		try {
			XmlRpcClient server = new XmlRpcClient(server_url);

			// Build our parameter list.
			Vector params = new Vector();
			params.addElement(new String(filePath));
			params.addElement(new String(fileName));


			// Call the server, and get our result.
			result =(String) server.execute("sample.getValueForInter", params);

		} catch (XmlRpcException exception) {
			System.err.println("JavaClient: XML-RPC Fault #" +
					Integer.toString(exception.code) + ": " +
					exception.toString());
		} catch (Exception exception) {
			System.err.println("JavaClient: " + exception.toString());
		}


		String data= new String();
		Scanner scanner = new Scanner(result);
		Map<String, Integer> map = new LinkedHashMap<String, Integer>();

		//		try {
		//			while((data=br.readLine())!=null) {
		//				String[] datainput = data.split(",");
		while(scanner.hasNextLine()) {

			String[] datainput = scanner.nextLine().split(",");
			String word = datainput[0];
			int count = Integer.parseInt(datainput[1]);
			if ( word!=null) {
				if(map.containsKey(word)) {
					map.put(word, map.get(word)+count);
				}else {
					map.put(word, count);
				}

			}
		}
		//	}

		//		catch (IOException e) {
		//			System.out.println(e);
		//		} 
		String redFile = filePathOutput+"\\"+fileName;
		//		File redfile = new File(redFile);
		//		BufferedWriter bw = new BufferedWriter(new FileWriter(redfile, true));
		StringBuilder sb = new StringBuilder();
		map.forEach((k, v) -> {
			//			try {

			//bw.write(k + "," + v+"\n");
			sb.append(k + "," + v+"\n");
			//			} catch (IOException e) {
			//				System.out.println("Couldn't write to file ");
			//			}
		});

		//	bw.close();

		try {
			XmlRpcClient server = new XmlRpcClient(server_url);

			// Build our parameter list.
			Vector params = new Vector();
			params.addElement(new String(filePathOutput));
			params.addElement(new String(fileName));
			params.addElement(new String(sb.toString()));


			// Call the server, and get our result.
			result =(String) server.execute("sample.writeDataToFile", params);

		} catch (XmlRpcException exception) {
			System.err.println("JavaClient: XML-RPC Fault #" +
					Integer.toString(exception.code) + ": " +
					exception.toString());
		} catch (Exception exception) {
			System.err.println("JavaClient: " + exception.toString());
		}


		return "Done";


	}
}
