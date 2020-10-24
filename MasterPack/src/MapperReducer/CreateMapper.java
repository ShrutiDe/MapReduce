package MapperReducer;
import java.io.IOException;
import java.util.Vector;

import helma.xmlrpc.XmlRpcClient;
import helma.xmlrpc.XmlRpcException;

public class CreateMapper extends Thread{
	int mapNum;
	String MapperIp;

	public CreateMapper(int mapNum,String MapperIp) {
		super();
		this.mapNum = mapNum;
		this.MapperIp = MapperIp;
	}

	@Override
	public void run() 
	{
			try {
				String server_url = "http://"+MapperIp+":3389";
				XmlRpcClient server = new XmlRpcClient(server_url);
				
				Vector params = new Vector();
				String master = ComputeEngineSample.getIP("master");
				params.addElement(new String(master));
				server.execute("sample.clientMain", params);
				

			} catch (XmlRpcException exception) {
				System.err.println("CreateMapper: XML-RPC Fault #" +
						Integer.toString(exception.code) + ": " +
						exception.toString());
			} catch (Exception exception) {
				System.err.println("CreateMapper: " + exception.toString());
			}

	}

}