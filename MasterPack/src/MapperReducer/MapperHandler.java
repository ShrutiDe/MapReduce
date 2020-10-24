package MapperReducer;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class MapperHandler extends Thread 
{ 

	final DataInputStream dis; 
	final DataOutputStream dos; 
	final Socket s; 
	final String dataPart;
	final int mapper;
	final int type;
	public MapperHandler(Socket s, DataInputStream dis, DataOutputStream dos, String dataPart, int mapper, int type) 
	{ 
		this.s = s; 
		this.dis = dis; 
		this.dos = dos;
		this.dataPart = dataPart;
		this.mapper = mapper;
		this.type = type;
	} 

	@Override
	public void run() 
	{ 	

		String received="";
		String toreturn=""; 

		try {
			String toSend = dataPart+"#"+Integer.toString(mapper)+Integer.toString(type);
			dos.writeUTF(toSend);
			toreturn = dis.readUTF();
			FaultCheck.fillMapStatus(mapper,toreturn);

			System.out.println(toreturn);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		try
		{ 
			this.dis.close(); 
			this.dos.close(); 

		}catch(IOException e){ 
			e.printStackTrace(); 
		} 
	} 
	


} 