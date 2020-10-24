package MapperReducer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ReducerHandler extends Thread 
{ 

	final DataInputStream dis; 
	final DataOutputStream dos; 
	final Socket s; 
	final String dataPart;
	final int reducer;
	final int type;
	public ReducerHandler(Socket s, DataInputStream dis, DataOutputStream dos, String dataPart, int reducer, int type) 
	{ 
		this.s = s; 
		this.dis = dis; 
		this.dos = dos;
		this.dataPart = dataPart;
		this.reducer = reducer;
		this.type = type;
	} 

	@Override
	public void run() 
	{ 	

		String received="";
		String toreturn=""; 

		try {
			dos.writeUTF(dataPart+"#"+Integer.toString(reducer)+Integer.toString(type));
			toreturn = dis.readUTF(); 
			FaultCheck.fillRedStatus(reducer,toreturn);

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