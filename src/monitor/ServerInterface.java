package monitor;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Date;
import java.util.Map;
import java.util.SortedMap;

public interface ServerInterface extends Remote{
    enum Transform {RAW, 
	    AVERAGE_MINUTE, AVERAGE_HOUR, AVERAGE_DAY, 
	    MEAN_MINUTE, MEAN_HOUR, MEAN_DAY,
	    MAX_MINUTE, MAX_HOUR, MAX_DAY,
	    MIN_MINUTE, MIN_HOUR, MIN_DAY,
	    PER_SECOND, PER_MINUTE, PER_HOUR, PER_DAY};
    enum Granularity {SECOND, MINUTE, HOUR, DAY}
    Map<String, SortedMap<Date, Double>> rawData(Date from, Date to, Iterable<String> nameSpec, Transform transform, Granularity granularity) throws RemoteException;
    Map<String, SortedMap<Date, Double>> rawLiveData(Iterable<Map<String, String>> names, Transform transform) throws RemoteException;
    Iterable<Map<String, String>> rawLiveNames() throws RemoteException;
    Iterable<Map<String, String>> rawNames(Date from, Date to) throws RemoteException;
    void ping() throws RemoteException;
}
