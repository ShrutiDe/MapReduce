
import java.io.BufferedReader;



import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import helma.xmlrpc.WebServer;
import org.apache.log4j.Logger;
public class KeyValueStore {
	 static Logger log = Logger.getLogger(KeyValueStore.class.getName());
	public static void main (String[] args) {
		try {
			 log.debug("KeyValueStore started with IP"+"34.122.223.204");
			  
			WebServer server = new WebServer(3389);
			server.addHandler("sample", new KeyValueStore());

		} catch (Exception exception) {
			System.err.println("JavaServer: " + exception.toString());
		}
	}


	public String getValueFromData(String filePath ,String key) throws FileNotFoundException {
		// TODO Auto-generated method stub
		 log.debug("Fetching Data");
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
		 log.debug("Fetching Data for combiner");
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

		 log.debug("Reading data from file");
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


	public String writeDataToFile(String folderName ,String fileName ,String value) throws IOException {
		 log.debug("Writing data to file");
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

		return "Done";
	}

}
