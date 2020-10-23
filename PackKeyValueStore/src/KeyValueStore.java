
import java.io.BufferedReader;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import helma.xmlrpc.WebServer;

public class KeyValueStore {

	public static void main (String[] args) {
		try {

			// Invoke me as <http://localhost:8080/RPC2>.
			WebServer server = new WebServer(8090);
			server.addHandler("sample", new KeyValueStore());

		} catch (Exception exception) {
			System.err.println("JavaServer: " + exception.toString());
		}
	}


	public String getValueFromData(String filePath ,String key) throws FileNotFoundException {
		// TODO Auto-generated method stub

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filePath+"\\"+key));
		} catch (FileNotFoundException e) {
			System.out.println("Problem while reading data from book" + e);
		}
		StringBuilder sb = new StringBuilder();
		try {
			String part;
			while(null != (part=br.readLine())) {
				sb.append(part);
				sb.append(" ");
			}
		} catch (IOException e) {
			System.out.println(e);
		}
		return sb.toString();
	}
	
	public String getValueForInter(String filePath ,String key) throws FileNotFoundException {
		// TODO Auto-generated method stub

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filePath+"\\"+key));
		} catch (FileNotFoundException e) {
			System.out.println("Problem while reading data from book" + e);
		}
		StringBuilder sb = new StringBuilder();
		try {
			String part;
			while(null != (part=br.readLine())) {
				sb.append(part+"\n");
			}
		} catch (IOException e) {
			System.out.println(e);
		}
		return sb.toString();
	}

	public LinkedHashMap<String, String> readDataFromFile(String fileName) throws FileNotFoundException {


		LinkedHashMap<String, String> hashMap = new LinkedHashMap<String, String>();
		File file = new File(fileName);
		boolean exists = file.exists();
		if (!exists) {
			System.out.println("No File Found");
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Could'nt create new file");
			}
		}
		BufferedReader br = new BufferedReader(new FileReader(file.getAbsoluteFile()));
		String data= new String();
		try {
			while((data=br.readLine())!=null) {
				String[] datainput = data.split(",");
				if ( datainput[0]!=null && datainput[1]!=null) {
					hashMap.put(datainput[0], datainput[1]);
				}
			}
		} catch (IOException e) {
			System.out.println(e);
		} 

		return hashMap;
	}

	//	public String writeHashMapToFile(String folderName ,String fileName ,String value) throws IOException {
	//		
	//		 File file = new File(folderName+"\\"+fileName);
	//		 boolean exists = file.exists();
	//		 if (!exists) {
	//	         try {
	//	        	 file.createNewFile();
	//			} catch (IOException e) {
	//				// TODO Auto-generated catch block
	//				 System.out.println("Could'nt create new file");
	//			}
	//	     }
	//		 
	//		 BufferedWriter bw = new BufferedWriter(new FileWriter(folderName+"\\"+fileName, false));
	//
	//				try {
	//					
	//					bw.write(value);
	//				} catch (IOException e) {
	//					System.out.println("Couldn't write to file ");
	//				}
	//		
	//
	//			bw.close();
	//		 
	////		Map<String, String> data = new LinkedHashMap<String, String>();
	////       data.put(fileName, value);
	////       BufferedWriter bw = new BufferedWriter(new FileWriter(folderName+"\\"+fileName, false));
	////       data.forEach((k, v) -> {
	////           try {
	////                  	
	////           	
	////               bw.write(k + "," + v+"\n");
	////           } catch (IOException e) {
	////              System.out.println("Couldn't write to file ");
	////           }
	////       });
	////
	////       bw.close();
	//		
	////		if(getValueFromData(keyId).equals("NOT FOUND"))
	////			return "NOT-STORED\r\n";
	////		else
	////			return "STORED\\r\\n";
	//		
	//		return "Done";
	//	}

	public String writeDataToFile(String folderName ,String fileName ,String value) throws IOException {

		File file = new File(folderName+"\\"+fileName);
		boolean exists = file.exists();
		if (!exists) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Could'nt create new file");
			}
		}

		BufferedWriter bw = new BufferedWriter(new FileWriter(folderName+"\\"+fileName, true));
		try {
			bw.write(value);
		} catch (IOException e) {
			System.out.println("Couldn't write to file ");
		}


		bw.close();

		//		if(getValueFromData(keyId).equals("NOT FOUND"))
		//			return "NOT-STORED\r\n";
		//		else
		//			return "STORED\\r\\n";

		return "Done";
	}

}
