package se.sj.monitor;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;



public class Test {
public static void main(String[] args) throws UnknownHostException, IOException, ClassNotFoundException {
	//ServerInterface.ServerClassLoader cl = new ServerInterface.ServerClassLoader(Thread
		//	.currentThread().getContextClassLoader());
	//Thread.currentThread().setContextClassLoader(cl);
	Socket s = new Socket(InetAddress.getLocalHost(), 3030);
	ObjectInputStream os = new ObjectInputStream(s.getInputStream());
	ServerInterface server = (ServerInterface) os.readObject();
	//cl.enable(server);
	System.out.println(server.rawLiveNames());
	Map<String, String> names =new HashMap<String,String>();
	names.put("instance", "0");
	System.out.println(server.rawLiveData(names));
	Date d1 =new Date(System.currentTimeMillis()-60*1000);
	Date d2=new Date();
	System.out.println(server.rawNames(d1, d2));
	System.out.println(server.rawData(d1, d2, Arrays.asList("instance", "0")));
}
}
