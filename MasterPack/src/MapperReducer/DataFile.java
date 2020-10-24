package MapperReducer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import helma.xmlrpc.XmlRpcClient;
import helma.xmlrpc.XmlRpcException;

public class DataFile {
	public BufferedReader readConfig() {
		BufferedReader fs = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/config.properties")));
		return fs;	
	}
	public String readBook(String fileName) {

		
		
		BufferedReader br = null;
		br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(fileName)));
		StringBuilder sb = new StringBuilder();
		try {
			String data;
			while(null != (data=br.readLine())) {
				sb.append(data);
				sb.append(" ");
			}
		} catch (IOException e) {
			System.out.println(e);
		}



		return sb.toString().toLowerCase() ;
	}

	public List<String> divideBook(String folderName, String text, int mapperCount, int type) throws IOException {
		BufferedReader fs = new DataFile().readConfig();


		Properties props = new Properties();
		props.load(fs);
		String KeyValueIP = ComputeEngineSample.getIP("keyvaluestore");
		String server_url = "http://"+KeyValueIP+":3389";
		
		if(type == 0) {
			text = text.replaceAll("[^a-z]+", " ");
			// TODO Auto-generated method stub


			int size = text.length();
			int blockSize = size/mapperCount;
			//			System.out.println(size+"size"+blockSize);
			List<String> ret = new ArrayList<String>();
			String empty= "";
			//			System.out.println(text);
			for(String word : text.split(" ")) {
				//System.out.println(word);
				if(empty.length()+word.length()<blockSize) {
					empty += word+" ";
				}else {

					ret.add(empty);
					empty=word + ' ';
				}
			}
			if(!empty.equals("")) {
				String add = ret.get(ret.size()-1)+empty;
				ret.remove(ret.size()-1);
				ret.add(ret.size()-1,add);
			}
			System.out.println(ret.size());
			List<String> files = new ArrayList();

			
			try {
				XmlRpcClient server = new XmlRpcClient(server_url);

				// Build our parameter list.
				for(int i=0;i<ret.size();i++) {
					String mapInput ="MapInput"+i+".txt";
					Vector params = new Vector();
					params.addElement(new String(folderName));
					params.addElement(new String(mapInput));
					params.addElement(new String(ret.get(i)));


					// Call the server, and get our result.
					String result =(String) server.execute("sample.writeDataToFile", params);
				}
			} catch (XmlRpcException exception) {
				System.err.println("JavaClient: XML-RPC Fault #" +
						Integer.toString(exception.code) + ": " +
						exception.toString());
			} catch (Exception exception) {
				System.err.println("JavaClient: " + exception.toString());
			}


			for(int i=0;i<ret.size();i++) {
				//System.out.println(i);
				String mapInput ="MapInput"+i+".txt";
				//
				//								BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(folderName+"\\"+mapInput,true)));
				//								bw.write(ret.get(i));
				//								bw.close();
				files.add(mapInput);
			}
			return files;
		}else {

			String[] fileNames = text.split(",");
			int size = fileNames.length;
			int numOfFiles = size/mapperCount;

//			List<List<String>> ret = new ArrayList<>();
//
//			final int chunkSize = 3;
//			final AtomicInteger counter = new AtomicInteger();
			StringBuilder sb = new StringBuilder();
			for (String file : fileNames) {
				BufferedReader br = new BufferedReader(new FileReader(file));



				String data;
				while(null != (data=br.readLine())) {
					sb.append(data);
					sb.append(" ");
				}
				//					if (counter.getAndIncrement() % mapperCount == 0) {
				//						ret.add(new ArrayList<>());
				//					}
				//					ret.get(ret.size() - 1).add(number);
			}
			String data = sb.toString();
			data = data.replaceAll("[^a-z ]+"," ");
			int sizeI = data.length();
			int blockSize = sizeI/mapperCount;
			//			System.out.println(size+"size"+blockSize);
			List<String> retI = new ArrayList<String>();
			String empty= "";
			//			System.out.println(text);
			for(String word : data.split(" ")) {
				//System.out.println(word);
				if(empty.length()+word.length()<blockSize) {
					empty += word+" ";
				}else {

					retI.add(empty);
					empty=word + ' ';
				}
			}
			if(!empty.equals("")) {
				String add = retI.get(retI.size()-1)+empty;
				retI.remove(retI.size()-1);
				retI.add(retI.size()-1,add);
			}
			System.out.println(retI.size());
			List<String> files = new ArrayList();

			try {
				XmlRpcClient server = new XmlRpcClient(server_url);

				// Build our parameter list.
				for(int i=0;i<retI.size();i++) {
					String mapInput ="MapInput"+i+".txt";
					Vector params = new Vector();
					params.addElement(new String(folderName));
					params.addElement(new String(mapInput));
					//params.addElement(new String(retI.get(i)));
					if(fileNames.length<=i)
						params.addElement(new String(fileNames[fileNames.length-1]+" "+retI.get(i)));
					else
						params.addElement(new String(fileNames[i]+" "+retI.get(i)));

						
					// Call the server, and get our result.
					String result =(String) server.execute("sample.writeDataToFile", params);
				}
			} catch (XmlRpcException exception) {
				System.err.println("JavaClient: XML-RPC Fault #" +
						Integer.toString(exception.code) + ": " +
						exception.toString());
			} catch (Exception exception) {
				System.err.println("JavaClient: " + exception.toString());
			}


			for(int i=0;i<retI.size();i++) {
				//System.out.println(i);
				String mapInput ="MapInput"+i+".txt";
				//
				//								BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(folderName+"\\"+mapInput,true)));
				//								bw.write(ret.get(i));
				//								bw.close();
				files.add(mapInput);
			}

//			List<String> result = new ArrayList();
//			for(List<String> lists : ret) {
//				String value = lists.stream()
//						.map(n -> String.valueOf(n))
//						.collect(Collectors.joining(",", "", ""));
//
//				result.add(value);
//			}
			return files;
		}

	}


}
