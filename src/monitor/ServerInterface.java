package monitor;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Date;
import java.util.Map;
import java.util.SortedMap;

public interface ServerInterface extends Remote{
    enum Transform {Raw, Average, Mean, Max, Min, PerSecond, PerMinute, PerHour, PerDay};
    enum Granularity {Second, Minute, Hour, Day}
    Map<String, SortedMap<Date, Double>> rawData(Date from, Date to, Iterable<String> nameSpec, Transform transform, Granularity granularity) throws RemoteException;
    Map<String, SortedMap<Date, Double>> rawLiveData(Iterable<Map<String, String>> names, Transform transform) throws RemoteException;
    Iterable<Map<String, String>> rawLiveNames() throws RemoteException;
    Iterable<Map<String, String>> rawNames(Date from, Date to) throws RemoteException;
    void ping() throws RemoteException;
}
