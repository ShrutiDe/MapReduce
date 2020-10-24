package MapperReducer;
import java.io.IOException;
import java.util.Vector;

import helma.xmlrpc.XmlRpcClient;
import helma.xmlrpc.XmlRpcException;

public class CreateReducer extends Thread{
	int redNum;
	String ReducerIp;

	public CreateReducer(int redNum,String ReducerIp) {
		super();
		this.redNum = redNum;
		this.ReducerIp = ReducerIp;
	}

	@Override
	public void run() 
	{

		try {
			String server_url = "http://"+ReducerIp+":3389";
			XmlRpcClient server = new XmlRpcClient(server_url);
			Vector params = new Vector();
			String master = ComputeEngineSample.getIP("master");
			params.addElement(new String(master));
			server.execute("sample.reducerMain", params);
			

		} catch (XmlRpcException exception) {
			System.err.println("CreateMapper: XML-RPC Fault #" +
					Integer.toString(exception.code) + ": " +
					exception.toString());
		} catch (Exception exception) {
			System.err.println("CreateMapper: " + exception.toString());
		}


	}

}