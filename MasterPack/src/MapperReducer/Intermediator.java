package MapperReducer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import helma.xmlrpc.XmlRpcClient;
import helma.xmlrpc.XmlRpcException;


public class Intermediator {
	static List<String> RedInputFiles = new ArrayList<String>();
	
	public BufferedReader readConfig() {
		BufferedReader fs = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/config.properties")));
		return fs;	
	}
	
	public static List<String> callIntermediateFunction(int mapNum, int redNum, int type, String intermediateInputFolder, String reducerInputFolder) throws IOException {
		// TODO Auto-generated method stub

		for(int i=0;i<mapNum;i++) {
			String fileName;
			if(type==0)
				fileName = "MapInput"+Integer.toString(i)+".json";
			else
				fileName =Integer.toString(i)+".json";
			combineFilesFromMapper(fileName,redNum,reducerInputFolder,intermediateInputFolder);
		}
		return RedInputFiles;
		//createReducerInputFiles(redNum);
	}

	private BufferedReader readFile(String fileName, String intermediateInputFolder) {
		// TODO Auto-generated method stub
		BufferedReader br=null;
		try {
			br = new BufferedReader(new FileReader(intermediateInputFolder+"\\"+fileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return br;
	}

	private static void combineFilesFromMapper(String fileName, int redNum, String reducerInputFolder, String intermediateInputFolder) throws IOException {

		//		File file = new File(intermediateInputFolder+"\\"+fileName);
		//
		//		BufferedReader br = null;
		//		br = new Intermediator().readFile(fileName,intermediateInputFolder);
		BufferedReader fs = new Intermediator().readConfig();


		Properties props = new Properties();
		props.load(fs);
		String KeyValueIP = props.getProperty("KeyValueIP");
		String server_url = "http://"+KeyValueIP+":3389";
		String result= new String();
		try {
			XmlRpcClient server = new XmlRpcClient(server_url);

			// Build our parameter list.
			Vector params = new Vector();
			params.addElement(new String(intermediateInputFolder));
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
		StringBuilder sb = new StringBuilder();
		Scanner scanner = new Scanner(result);
		Map<String,String> map = new HashMap();	
		while(scanner.hasNextLine()) {

			String[] datainput = scanner.nextLine().split(",");
			//		String data=new String();
			//		while((data=br.readLine())!=null) {
			//			String[] datainput = data.split(",");
			if (!datainput[0].equals("") && datainput.length>1 ) {
				//try {
					int hashValue = (datainput[0].charAt(0)-'a')%redNum;
					String interName = Integer.toString(hashValue)+"Inter"+".json";
					if(!RedInputFiles.contains(interName)) {
						RedInputFiles.add(interName);
					}
					if(map.containsKey(interName)) {
						map.put(interName, map.get(interName)+datainput[0] + "," + datainput[1]+"\n");
					}else {
						map.put(interName, datainput[0] + "," + datainput[1]+"\n");
					}

					//					BufferedWriter bw = new BufferedWriter(new FileWriter(reducerInputFolder+"\\"+interName, true));
					//					bw.write(datainput[0] + "," + datainput[1]+"\n");
					//					bw.close();
//				} catch (IOException e) {
//					System.out.println("Couldn't write to file ");
//				}
			}
		} 
		
		try {
			XmlRpcClient server = new XmlRpcClient(server_url);
			//String result1= new String();
			
			map.forEach((k,v)->{
				Vector params = new Vector();
				params.addElement(new String(reducerInputFolder));
				params.addElement(new String(k));
				params.addElement(new String(v));
				//result1 =(String) server.execute("sample.writeDataToFile", params);
				try {
					server.execute("sample.writeDataToFile", params);
				} catch (XmlRpcException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
				
		} catch (Exception exception) {
			System.err.println("JavaClient: " + exception.toString());
		}
		scanner.close();
	}


}
