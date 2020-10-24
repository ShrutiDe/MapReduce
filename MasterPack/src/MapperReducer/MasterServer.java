package MapperReducer;


import java.io.*;





import java.util.*;

import helma.xmlrpc.WebServer;

import java.net.*; 

public class MasterServer 
{ 
	int num_of_map;
	int num_of_red;
	public static void main (String [] args) {
		try {

			// Invoke me as <http://localhost:8080/RPC2>.
			WebServer server = new WebServer(3389);
			server.addHandler("sample", new MasterServer());

		} catch (Exception exception) {
			System.err.println("JavaServer: " + exception.toString());
		}
	}

	public String run_mapred(String input_data, String map_fn, String reduce_fn, String output_location) throws IOException {



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
		String ReducerInput = output_location;


		ServerSocket ss = new ServerSocket(Integer.parseInt(MasterPort));

		System.out.println("Application Started");
		if(type == 0) {

			System.out.println(mapNum+" Mappers started");
			System.out.println();
			String MapperIp=null;
			for(int i=0;i<mapNum ;i++) {
				ComputeEngineSample.createInstance("mapper"+mapNum);
				MapperIp = ComputeEngineSample.getIP("mapper"+mapNum);
				Thread th = new CreateMapper(mapNum,MapperIp);
				th.start();
			}



			DataFile data = new DataFile();
			System.out.println("File Reading in Progress!!");
			String text = data.readBook(fileName);


			System.out.println("File is divided into chunks as described in report");
			System.out.println();
			List<String> bookParts = data.divideBook(MapperInputFolder,text,mapNum,type);

			int count = 0;
			System.out.println("Data is transfered to individual mappers parallelly");
			System.out.println();
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
				System.out.println("Mappers Failed :"+fault.size());

				Thread t = new CreateMapper(fault.size(),MapperIp);
				t.start();
				List<String> leftData = new ArrayList();
				for(int i : fault) {
					leftData.add(bookParts.get(i));
				}

				sendData(count, fault.size(),leftData,ss, type);
				System.out.println("Mapper recovered");
			}
			System.out.println();
			System.out.println("Mappers Completed");
			System.out.println();


			System.out.println("Combiner function called to process the data from Mapper and produce the input files for Reducer");
			System.out.println("");
			List<String> RedInputFiles = new ArrayList<String>();
			RedInputFiles =Intermediator.callIntermediateFunction(mapNum,redNum, type,IntermediateInputFolder,ReducerInput);

			System.out.println(redNum+" Reducers started");
			System.out.println();

			String ReducerIp=null;
			for(int i=0;i<mapNum ;i++) {
				ComputeEngineSample.createInstance("reducer"+mapNum);
				ReducerIp = ComputeEngineSample.getIP("reducer"+mapNum);
				Thread tr = new CreateReducer(redNum,ReducerIp);
				tr.start();
			}
			
			
			count = 0;
			System.out.println("Data is transfered to individual Reducers parallelly");
			System.out.println();
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

				System.out.println("Reducers Failed :"+result.size());
				Thread rt = new CreateReducer(result.size(),ReducerIp);
				rt.start();
				List<String> leftData = new ArrayList();
				for(int i : fault) {
					leftData.add(RedInputFiles.get(i));
				}


				sendData(count, result.size(),RedInputFiles,ss, type);
				System.out.println("Reducers recovered");
				System.out.println();
				return "Done";
				//return true;
			}else {
				System.out.println();
				System.out.println("Reducers Completed");
				System.out.println();
				return "Done";
				//return false;
			}

			//////////////////////////////////////////////////////////////////////////////////////////////////////
		}else {
			DataFile data = new DataFile();
			//String text = data.readBook(type);
			System.out.println("File Reading in Process!!");
			System.out.println();
			List<String> bookParts = data.divideBook(MapperInputFolder,indexFiles,mapNum,type);

			int count = 0;

			//  Create mappers
			System.out.println(bookParts.size()+" Mappers started");
			System.out.println();
			String MapperIp=null;
			for(int i=0;i<mapNum ;i++) {
				ComputeEngineSample.createInstance("mapper"+mapNum);
				MapperIp = ComputeEngineSample.getIP("mapper"+mapNum);
				Thread th = new CreateMapper(mapNum,MapperIp);
				th.start();
			}

			System.out.println("File is divided into chunks as described in report");
			System.out.println();
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
				System.out.println("Mappers Failed are"+fault.size());

				Thread t = new CreateMapper(fault.size(),MapperIp);
				t.start();
				List<String> leftData = new ArrayList();
				for(int i : fault) {
					leftData.add(bookParts.get(i));
				}
				sendData(count, fault.size(),leftData,ss, type);
				System.out.println("Mapper recovered");
			}
			System.out.println("Mappers Completed");
			System.out.println();

			System.out.println("Combiner function called to process the data from Mapper and produce the input files for Reducer");
			System.out.println("");
			List<String> RedInputFiles = new ArrayList<String>();
			RedInputFiles =Intermediator.callIntermediateFunction(bookParts.size(),redNum, type, IntermediateInputFolder,ReducerInput);

			System.out.println(redNum+" Reducers started");
			System.out.println();
			
			String ReducerIp=null;
			for(int i=0;i<mapNum ;i++) {
				ComputeEngineSample.createInstance("reducer"+mapNum);
				ReducerIp = ComputeEngineSample.getIP("reducer"+mapNum);
				Thread tr = new CreateReducer(redNum,ReducerIp);
				tr.start();
			}
			int count1 =0;
			System.out.println("Data is transfered to individual Reducers parallelly");
			System.out.println();
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
				System.out.println("Reducers Failed are"+faults.size());
				System.out.println("Fault Tolerance feature on");
				System.out.println();

				Thread rt = new CreateReducer(faults.size(),ReducerIp);
				rt.start();
				List<String> leftData = new ArrayList();
				for(int i : faults) {
					leftData.add(RedInputFiles.get(i));
				}
				sendData(count, faults.size(),RedInputFiles,ss, type);
				System.out.println("Reducers recovered");
				System.out.println();
				return "Done";
				//return true;
			}else {
				System.out.println("Reducers Completed");
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

				System.out.println("New client connected : " + s); 

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