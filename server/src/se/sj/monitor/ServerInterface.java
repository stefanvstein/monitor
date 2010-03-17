package se.sj.monitor;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Date;
import java.util.Map;
import java.util.SortedMap;



public interface ServerInterface extends Remote{
	
	Map<String, SortedMap<Date, Double>> rawData(Date from, Date to, Iterable<String> nameSpec)
	throws RemoteException;
	Map<String, SortedMap<Date, Double>> rawLiveData(Map<String, String> names)
	throws RemoteException;
	Iterable<Map<String, String>> rawLiveNames() throws RemoteException;
	Iterable<Map<String, String>> rawNames(Date from, Date to) throws RemoteException;
    void ping() throws RemoteException;
	byte[] classData(String name) throws RemoteException;

	public static class ServerClassLoader extends ClassLoader {

		
		private ServerInterface server;
		private boolean enabled = false;

		public ServerClassLoader(ClassLoader parent) {
			super(parent);


		}
		
		public void enable(ServerInterface server)
		{
			this.server = server;
			enabled=true;
		}

		@Override
		protected Class<?> findClass(String name) throws ClassNotFoundException {
			try {
				return super.findClass(name);
			} catch (ClassNotFoundException e) {
				if(!enabled)
					throw e;
				try {
					byte[] b = server.classData(name);
					return defineClass(name, b, 0, b.length);
				} catch (RemoteException e2) {
					throw new ClassNotFoundException("Could not find class "
							+ name, e2.getCause());
				}
			}

		}
	}

}
