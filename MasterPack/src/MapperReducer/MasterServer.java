package MapperReducer;


import java.io.*;





import java.util.*;

import org.apache.log4j.Logger;

import helma.xmlrpc.WebServer;

import java.net.*; 

public class MasterServer 
{ 
	int num_of_map;
	int num_of_red;
	
	 static Logger log = Logger.getLogger(MasterServer.class.getName());
	  
	public static void main (String [] args) {
		try {

			// Invoke me as <http://localhost:8080/RPC2>.
			WebServer server = new WebServer(3389);
			server.addHandler("sample", new MasterServer());

		} catch (Exception exception) {
			System.err.println("JavaServer: " + exception.toString());
		}
	}

	public String run_mapred(String input_data, String map_fn, String reduce_fn, String output_location) throws Exception {



		BufferedReader fs = new MasterServer().readConfig();


		Properties props = new Properties();
		props.load(fs);
		int mapNum = num_of_map;
		int redNum = num_of_red;
		int type = Integer.parseInt(props.getProperty("task"));

		String MasterPort = props.getProperty("MasterPort");
		String indexFiles = props.getProperty("Book1Name");
		String fileName = input_data;
		String MapperInputFolder = props.getProperty("MapperInputFolder");
		String IntermediateInputFolder = props.getProperty("IntermediateInputFolder");
		String ReducerInput = props.getProperty("ReducerInputFolder");
		String mapperStartupScript="mapper.sh";
		String reducerStartupScript="reducer.sh";


		ServerSocket ss = new ServerSocket(Integer.parseInt(MasterPort));

		log.debug("Application Started");
		if(type == 0) {

			log.debug(mapNum+" Mappers started");
			log.debug("");
			String MapperIp=null;
			for(int i=0;i<mapNum ;i++) {
				ComputeEngineSample.createInstance("mapper"+i,mapperStartupScript);
				MapperIp = ComputeEngineSample.getIP("mapper"+i);
				Thread th = new CreateMapper(mapNum,MapperIp);
				th.start();
			}



			DataFile data = new DataFile();
			log.debug("File Reading in Progress!!");
			String text = data.readBook(fileName);


			log.debug("File is divided into chunks as described in report");
			log.debug("");
			List<String> bookParts = data.divideBook(MapperInputFolder,text,mapNum,type);

			int count = 0;
			log.debug("Data is transfered to individual mappers parallelly");
			log.debug("");
			sendData(count, mapNum,bookParts,ss, type);

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


			String part = "Mapper";
			List<Integer> fault = FaultCheck.checkMapperStatus(part);

			if(fault.size()!=0) {
				log.debug("Mappers Failed :"+fault.size());
				ComputeEngineSample.createInstance("mapper"+mapNum,mapperStartupScript);
				MapperIp = ComputeEngineSample.getIP("mapper"+mapNum);
				Thread t = new CreateMapper(fault.size(),MapperIp);
				t.start();
				List<String> leftData = new ArrayList();
				for(int i : fault) {
					leftData.add(bookParts.get(i));
				}

				sendData(count, fault.size(),leftData,ss, type);
				log.debug("Mapper recovered");
			}
			log.debug("");
			log.debug("Mappers Completed");
			log.debug("");
			
			log.debug("Deleting instances");
			ComputeEngineSample.deleteInstances("mapper");

			log.debug("Combiner function called to process the data from Mapper and produce the input files for Reducer");
			log.debug("");
			List<String> RedInputFiles = new ArrayList<String>();
			RedInputFiles =Intermediator.callIntermediateFunction(mapNum,redNum, type,IntermediateInputFolder,ReducerInput);

			log.debug(redNum+" Reducers started");
			log.debug("");

			String ReducerIp=null;
			for(int i=0;i<mapNum ;i++) {
				
				ComputeEngineSample.createInstance("reducer"+mapNum,mapperStartupScript);
				ReducerIp = ComputeEngineSample.getIP("reducer"+mapNum);
				Thread tr = new CreateReducer(redNum,ReducerIp);
				tr.start();
			}
			
			
			count = 0;
			log.debug("Data is transfered to individual Reducers parallelly");
			log.debug("");
			sendData(count, redNum,RedInputFiles,ss, type);

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//log.debug("Post every reducer is done, fault tolerant process is carried out and remaining data is processed again ");
			String parts = "Reducer";
			List<Integer> result = FaultCheck.checkMapperStatus(part);
			if(result.size()!=0) {

				log.debug("Reducers Failed :"+result.size());
				Thread rt = new CreateReducer(result.size(),ReducerIp);
				rt.start();
				List<String> leftData = new ArrayList();
				for(int i : fault) {
					leftData.add(RedInputFiles.get(i));
				}


				sendData(count, result.size(),RedInputFiles,ss, type);
				log.debug("Reducers recovered");
				log.debug("");
				log.debug("Deleting instances");
				ComputeEngineSample.deleteInstances("reducer");
				return "Done";
				//return true;
			}else {
				log.debug("");
				log.debug("Reducers Completed");
				log.debug("Deleting instances");
				ComputeEngineSample.deleteInstances("reducer");
				log.debug("");
				return "Done";
				//return false;
				
				
			}
	}else {
			DataFile data = new DataFile();
			//String text = data.readBook(type);
			log.debug("File Reading in Process!!");
			log.debug("");
			List<String> bookParts = data.divideBook(MapperInputFolder,indexFiles,mapNum,type);

			int count = 0;

			//  Create mappers
			log.debug(bookParts.size()+" Mappers started");
			log.debug("");
			String MapperIp=null;
			for(int i=0;i<mapNum ;i++) {
				ComputeEngineSample.createInstance("mapper"+mapNum,reducerStartupScript);
				MapperIp = ComputeEngineSample.getIP("mapper"+mapNum);
				Thread th = new CreateMapper(mapNum,MapperIp);
				th.start();
			}

			log.debug("File is divided into chunks as described in report");
			log.debug("");
			sendData(count, mapNum,bookParts,ss, type);

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			String part = "Mapper";
			List<Integer> fault = FaultCheck.checkMapperStatus(part);

			if(fault.size()!=0) {
				//class fault size mapper
				log.debug("Mappers Failed are"+fault.size());

				Thread t = new CreateMapper(fault.size(),MapperIp);
				t.start();
				List<String> leftData = new ArrayList();
				for(int i : fault) {
					leftData.add(bookParts.get(i));
				}
				sendData(count, fault.size(),leftData,ss, type);
				log.debug("Mapper recovered");
			}
			log.debug("Mappers Completed");
			log.debug("");
			log.debug("Deleting instances");
			ComputeEngineSample.deleteInstances("mapper");
			
			log.debug("Combiner function called to process the data from Mapper and produce the input files for Reducer");
			log.debug("");
			List<String> RedInputFiles = new ArrayList<String>();
			RedInputFiles =Intermediator.callIntermediateFunction(bookParts.size(),redNum, type, IntermediateInputFolder,ReducerInput);

			log.debug(redNum+" Reducers started");
			log.debug("");
			
			String ReducerIp=null;
			for(int i=0;i<mapNum ;i++) {
				ComputeEngineSample.createInstance("reducer"+mapNum,reducerStartupScript);
				ReducerIp = ComputeEngineSample.getIP("reducer"+mapNum);
				Thread tr = new CreateReducer(redNum,ReducerIp);
				tr.start();
			}
			int count1 =0;
			log.debug("Data is transfered to individual Reducers parallelly");
			log.debug("");
			sendData(count, redNum,RedInputFiles,ss, type);

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String part2 = "Reducer";
			List<Integer> faults = FaultCheck.checkMapperStatus(part);
			if(faults.size()!=0) {
				log.debug("Reducers Failed are"+faults.size());
				log.debug("Fault Tolerance feature on");
				log.debug("");

				Thread rt = new CreateReducer(faults.size(),ReducerIp);
				rt.start();
				List<String> leftData = new ArrayList();
				for(int i : faults) {
					leftData.add(RedInputFiles.get(i));
				}
				sendData(count, faults.size(),RedInputFiles,ss, type);
				log.debug("Reducers recovered");
				log.debug("");
				log.debug("Deleting instances");
				ComputeEngineSample.deleteInstances("reducer");
				return "Done";
				//return true;
			}else {
				log.debug("Reducers Completed");
				log.debug("Deleting instances");
				ComputeEngineSample.deleteInstances("reducer");
				return "Done";
				//return false;
			}

		}

	}




	public void  int_cluster(int num_of_map, int num_of_red) 
	{
		this.num_of_map = num_of_map;
		this.num_of_red = num_of_red;

	} 

	public String destroy_cluster(String cluster_id)
	{
		return cluster_id;

	}


	public static void sendData(int count, int num,List<String> data, ServerSocket ss, int type) throws IOException {
		while (count<num) 
		{ 
			Socket s = null; 

			try
			{ 
				s = ss.accept(); 

				log.debug("New client connected : " + s); 

				DataInputStream dis = new DataInputStream(s.getInputStream()); 
				DataOutputStream dos = new DataOutputStream(s.getOutputStream());
				Thread t = new MapperHandler(s, dis, dos,data.get(count), count, type); 
				count++;

				t.start(); 



			} 
			catch (Exception e){ 
				s.close(); 
				e.printStackTrace(); 
			} 
		}

	}


	public BufferedReader readConfig() {
		BufferedReader fs = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/config.properties")));
		return fs;	
	}

}