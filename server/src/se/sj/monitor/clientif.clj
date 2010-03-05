(ns se.sj.monitor.clientif
  (:import (java.io ByteArrayOutputStream IOException ObjectOutputStream ObjectInputStream))
  (:import (se.sj.monitor ServerInterface))
  (:import (java.rmi.server UnicastRemoteObject))  
  (:import (java.rmi RemoteException NoSuchObjectException))
  (:import (java.net ServerSocket Socket InetAddress SocketTimeoutException))
  (:use (clojure stacktrace test))
  (:use (clojure.contrib str-utils duck-streams))
  (:use (se.sj.monitor database)))

(defn names-as-keyworded [names]
  (reduce (fn [result name]
	    (assoc result (keyword (key name)) (val name))) 
	  {} names))

(defn keyworded-names-as-string [names]
  (reduce (fn [result a-name]
	    (assoc result (name (key a-name)) (val a-name))) 
	  {} names))

(defn- byte-code-for [name]
  (try
   (let [clazz (. (.getClassLoader clojure.lang.PersistentArrayMap) loadClass name)
	 resource (str (re-gsub #"\." "/" (.getName clazz)) ".class")
	 outputStream (ByteArrayOutputStream.)]
     (with-open [input (.. clazz (getClassLoader) (getResourceAsStream resource))
		 output outputStream]
       (copy input output))
     (.toByteArray outputStream))
   (catch Exception e
     (cond 
      (instance? ClassNotFoundException (root-cause e)) 
      (throw (RemoteException. "Remote class not found" e))
      (instance? IOException (root-cause e)) 
      (throw (RemoteException. "Bytecode for remote class could not be loaded" e))))))

(defn- raw-data [from to names]
  (when (odd? (count names))
    (throw (IllegalArgumentException. "Odd number of names")))
  (let [keyed-names (interleave 
		     (map #(keyword (first %)) (partition 2 names)) 
		     (map #(second %) (partition 2 names)))
	result (apply data-by from to keyed-names)]
  (reduce (fn [result row]
	      (assoc result (keyworded-names-as-string (key row)) (val row)))
	  {} result)))


(defn- raw-live-data [names]
  (let [keyed-names (names-as-keyworded names)
       result (data-by (fn [a-name] 
			 (every? (fn [requirement] 
				   (= ((key requirement) a-name) 
				      (val requirement))) 
				 keyed-names)))]
    (reduce (fn [result row]
	      (assoc result (keyworded-names-as-string (key row)) (val row)))
	     {} result)))

(defn- raw-live-names []
  (reduce (fn [result a-name]
	    (conj result  (keyworded-names-as-string a-name))) 
	  #{} 
	  (names-where (fn [_] true))))
 
(defn- raw-names [from to]
  (reduce (fn [result a-name]
	    (conj result  (keyworded-names-as-string a-name))) 
	  #{} 
	  (names-where (fn [_] true) from to)))

(defmacro serve-clients [port transfer-port stop-fn & form]
  `(let [remote# (atom nil)] 
     (try 
      (let [obj#
	    (UnicastRemoteObject/exportObject 
	     (proxy [ServerInterface] []
	       (rawData [from# to# names#] 
			(raw-data from# to# names#))
	       (rawLiveData [names#]
			    (raw-live-data names#))
	       (rawLiveNames []
			     (raw-live-names))
	       (rawNames [from# to#]
			 (raw-names from# to#))
	       (classData [name#]
			  (byte-code-for name#))
	       ) ~port)]
	(swap! remote# (fn [_#] obj#)))
      (let [server-socket# (ServerSocket. ~transfer-port)]
	(. server-socket# setSoTimeout 1000)
	(.start (Thread. (fn []
			   (with-open [ss# server-socket#]
			   (while (not (~stop-fn))
				  (try
				   (with-open [sock# (.accept ss#)
					       output# (ObjectOutputStream. (.getOutputStream sock#))]
				     (. output# writeObject @remote#))
				   (catch SocketTimeoutException _#)))))))
	(do ~@form))
	 
      (finally (when @remote#
		 (try 
		  (UnicastRemoteObject/unexportObject @remote# true)
		  (catch NoSuchObjectException _#)))))))

(deftest simple
  (is (not (nil? (byte-code-for "clojure.lang.PersistentArrayMap")))))

(deftest keyworded
  (is (= {:Olle "Nisse" :Arne "Gulsot"} (names-as-keyworded {"Olle" "Nisse" "Arne" "Gulsot"})))
  (is (= {"Olle" "Nisse" "Arne" "Gulsot"} (keyworded-names-as-string {:Olle "Nisse" :Arne "Gulsot"}))))

(deftest test-raw-live
  (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (using-live
     (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100001") 1)
     (add-data {:host "Arne" :counter "Olle"} (dparse "20100101 100002") 2)
     (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100003") 3)
     (add-data {:host "Bengt" :counter "Olle"} (dparse "20100101 100004") 4)
     (is (= (raw-live-data {"host" "Arne"}) 
	    {{"host" "Arne" "counter" "Nisse"} { (dparse "20100101 100001") 1  
						 (dparse "20100101 100003") 3}
	     {"host" "Arne" "counter" "Olle"} { (dparse "20100101 100002") 2}})) 
     (is (= (raw-live-data {"host" "Arne" "counter" "Olle"}) 
	    {{"host" "Arne" "counter" "Olle"} { (dparse "20100101 100002") 2}})) 
     (is (= (raw-live-data {"counter" "Olle"}) 
	    {{"host" "Arne" "counter" "Olle"} { (dparse "20100101 100002") 2}
	     {"host" "Bengt" "counter" "Olle"} {(dparse "20100101 100004") 4}})) 
     (is (= (raw-live-data {"host" "Nisse"}) {}))
     (is (= (raw-live-names) #{{"host" "Arne" "counter" "Olle"} 
			       {"host" "Arne" "counter" "Nisse"}
			       {"host" "Bengt" "counter" "Olle"}}))
     (is (empty? 
	  (raw-data (dparse "20070101 010101") 
		    (dparse "20080101 010101") 
		    ["host" "Arne" "counter" "Nisse"]))))))
(comment
  (deftest test-serving
    (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	  dparse #(. df parse %)
      stop-signal (atom false)
      stop #(deref stop-signal)]
    (try
     (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100001") 1)
     (add-data {:host "Arne" :counter "Olle"} (dparse "20100101 100002") 2)
     (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100003") 3)
     (add-data {:host "Bengt" :counter "Olle"} (dparse "20100101 100004") 4)
     (serve-clients 2424 2425 stop 
		    (with-open [s (Socket. (InetAddress/getLocalHost) 2425)
				is (ObjectInputStream. (.getInputStream s))]
		      (let [server (.readObject is)]
			(println (.rawLiveNames server))
			)))
     (finally (swap! stop-signal (fn [_] true))
	      (Thread/sleep 1200)
	      (clean-live-data-older-than (dparse "20110101 010101")))
)))
)



