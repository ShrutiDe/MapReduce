

import java.io.*;


import java.net.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.stream.Collectors;

import javax.naming.spi.ResolveResult;

import org.apache.log4j.Logger;

import helma.xmlrpc.WebServer;
import helma.xmlrpc.XmlRpcClient;
import helma.xmlrpc.XmlRpcException;
import org.apache.log4j.Logger;

//Client class 
public class Mapper {
	int mapperFault;
	String filePath;
	String filePathOutput;

	String server_url;
	 static Logger log = Logger.getLogger(Mapper.class.getName());
	public static void main (String [] args) {
		try {
			log.debug("Mapper VM started");
			// Invoke me as <http://localhost:8080/RPC2>.
			WebServer server = new WebServer(3389);
			server.addHandler("sample", new Mapper());

		} catch (Exception exception) {
			System.err.println("JavaServer: " + exception.toString());
		}
	}

	public void clientMain(String ip) throws IOException {



		try {
			log.debug("Mapper process started with IP"+ip);
			BufferedReader fs = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/MapReduceConfig.properties")));
			Properties props = new Properties();
			props.load(fs);

			String KeyValueIP = props.getProperty("KeyValueIP");

			server_url = "http://"+KeyValueIP+":3389";
			log.debug("Mapper connected to KeyValueStore on IP"+KeyValueIP);
			int port = Integer.parseInt(props.getProperty("MapReducePort"));

			int mapperFault = Integer.parseInt(props.getProperty("mapperFault"));
			filePath = props.getProperty("MapperInputFolder");
			filePathOutput = props.getProperty("MapperOutputFolder");
			log.debug("Mapper connected to Master on IP"+ip);
			Socket s = new Socket(ip, port);

			DataInputStream dis = new DataInputStream(s.getInputStream());
			DataOutputStream dos = new DataOutputStream(s.getOutputStream());

			String received = dis.readUTF();
			String mapperNum = received.substring(received.length()-2,received.length()-1);

			if(mapperFault==Integer.parseInt(mapperNum)) {
				dos.writeUTF("Not Done");
			}else {

				int code = Integer.parseInt(received.substring(received.length()-1));

				received = received.substring(0, received.length()-1);

				String status=null;
				if(code == 0) {
					status = writeDataToFileWordCount(received);
				}else if(code == 1) {
					status = writeDataToFileInvertedIndex(received);
				}
				if(status.equals("Done"))
					dos.writeUTF("Done");
				else
					dos.writeUTF("Not Done");
			}
			log.debug("Mapper process done with IP"+ip);
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
		// key = word, value = set of filenames containing that word
		//		BufferedReader br = null;
		//		String[] files = received.split("#");
		//		int mapperName = Integer.parseInt(files[1]);

		//	String[] fileNames = files[0].split(",");
		Map<String, List<String>> map = new HashMap<String, List<String>>();



		String[] input = received.split("#");
		int mapperName = Integer.parseInt(input[1]);
		String fileforMapper = input[0];
		System.out.println("fileforMapper"+fileforMapper);
		String result=new String();
		try {
			XmlRpcClient server = new XmlRpcClient(server_url);

			// Build our parameter list.
			Vector params = new Vector();
			params.addElement(new String(filePath));
			params.addElement(new String(fileforMapper));


			// Call the server, and get our result.
			result =(String) server.execute("sample.getValueFromData", params);
			System.out.println(result);
		} catch (XmlRpcException exception) {
			System.err.println("JavaClient: XML-RPC Fault #" +
					Integer.toString(exception.code) + ": " +
					exception.toString());
		} catch (Exception exception) {
			System.err.println("JavaClient: " + exception.toString());
		}


		//String content = sb.toString().toLowerCase() ;
		String content = result.toLowerCase();
		String[] words1 = content.split(" ");
		String ogFile = words1[0];
		content = content.replaceAll("[^a-z]", " ");
		String[] words = content.split(" ");
		String fileStart = fileforMapper.substring(8,9);
		words[0]="";

		for(String word : words) {
			if(!word.equals("")) {
				if(map.containsKey(word)) {
					if(map.get(word).contains(ogFile))
						continue;
					else {
						List<String> set = map.get(word);
						set.add(ogFile);
						map.put(word,set);
					}
				}
				else {
					List<String> set = new ArrayList();
					set.add(ogFile);
					map.put(word,set);
				}
			}
		}
		StringBuilder sb1 = new StringBuilder();
		map.forEach((k, v) -> {
			if(v.size()!=0 && !k.equals("")) {

				//	try {
				String value = v.stream()
						.map(n -> String.valueOf(n))
						.collect(Collectors.joining(":", "", ""));

				//bw.write(k + "," +value +"\n");
				sb1.append(k + "," +value +"\n");
				//				} catch (IOException e) {
				//					System.out.println("Couldn't write to file ");
				//				}
			}
		});

		//bw.close();

		try {
			XmlRpcClient server = new XmlRpcClient(server_url);

			// Build our parameter list.
			Vector params = new Vector();
			params.addElement(new String(filePathOutput));
			params.addElement(new String(mapperName+".json"));
			params.addElement(new String(sb1.toString()));


			// Call the server, and get our result.
			String result1 =(String) server.execute("sample.writeDataToFile", params);

		} catch (XmlRpcException exception) {
			System.err.println("JavaClient: XML-RPC Fault #" +
					Integer.toString(exception.code) + ": " +
					exception.toString());
		} catch (Exception exception) {
			System.err.println("JavaClient: " + exception.toString());
		}


		return "Done";
	}

	public String writeDataToFileWordCount(String data) throws IOException {

		String[] input = data.split("#");

		String fileforMapper = input[0];		

		String result=null;
		try {
			XmlRpcClient server = new XmlRpcClient(server_url);

			// Build our parameter list.
			Vector params = new Vector();
			params.addElement(new String(filePath));
			params.addElement(new String(fileforMapper));


			// Call the server, and get our result.
			result =(String) server.execute("sample.getValueFromData", params);

		} catch (XmlRpcException exception) {
			System.err.println("JavaClient: XML-RPC Fault #" +
					Integer.toString(exception.code) + ": " +
					exception.toString());
		} catch (Exception exception) {
			System.err.println("JavaClient: " + exception.toString());
		}


		//String content = sb.toString().toLowerCase() ;
		String content = result.toLowerCase();
		content = content.replaceAll("[^a-z]", " ");
		String[] words = content.split(" ");
		String fileStart = fileforMapper.substring(8,9);

		String fileName = "MapInput"+fileStart+".json";
		File file = new File(filePathOutput+"\\"+fileName);
		boolean exists = file.exists();
		if (!exists) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Could'nt create new file");
			}	
		}
		Map<String, Integer> map = new LinkedHashMap<String, Integer>();

		for(String word :  words) {
			if(word.equals("")) 
				continue;
			if(map.containsKey(word)) {
				map.put(word, map.get(word)+1);

			}else {
				map.put(word, 1);
			}

		}
		StringBuilder sb = new StringBuilder();
		map.forEach((k, v) -> {
			sb.append(k + "," + v+"\n");
		});

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
