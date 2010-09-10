(ns se.sj.monitor.clientif
  (:import (java.io ByteArrayOutputStream IOException ObjectOutputStream ObjectInputStream))
  (:import (se.sj.monitor ServerInterface))
  (:import (java.rmi.server UnicastRemoteObject))  
  (:import (java.rmi RemoteException NoSuchObjectException))
  (:import (java.net ServerSocket Socket InetAddress SocketTimeoutException))
(:import (java.util HashMap TreeMap))
  (:use (clojure stacktrace test))
  (:use (clojure.contrib str-utils duck-streams profile))
  (:use (se.sj.monitor database))
  (:use cupboard.utils))

(defn- names-as-keyworded [names]
  (reduce (fn [result name]
	    (assoc result (keyword (key name)) (val name))) 
	  {} names))

(defn- keyworded-names-as-string [names]
 (let [result (reduce (fn [result a-name]
	    (conj result (name (key a-name)) (val a-name))) 
	  [] names)]
(apply array-map result)))

(defn raw-data [from to names]
  (when (odd? (count names))
    (throw (IllegalArgumentException. "Odd number of names")))
  (let [keyed-names (interleave 
		     (map #(keyword (first %)) (partition 2 names)) 
		     (map #(second %) (partition 2 names)))
	result (apply data-by from to keyed-names)
	final-result (HashMap.)]
    (dorun (map (fn [row]
		  (. final-result put (HashMap. #^java.util.Map (keyworded-names-as-string (key row))) (TreeMap. #^java.util.Map (val row))))
	
	    result))
    final-result))


(defn raw-live-data [names]
  (let [keyworded-names (reduce (fn [r i] (conj r (names-as-keyworded i))) [] names)
	result (reduce (fn [r i] (merge r (data-by (fn [a-name] 
			 (every? (fn [requirement] 
				   (= ((key requirement) a-name) 
				      (val requirement))) 
				 i))))) {} keyworded-names)
		final-result (HashMap.)] 
    (dorun (map (fn [row]
		  (. final-result put (HashMap. #^java.util.Map (keyworded-names-as-string (key row))) (TreeMap. #^java.util.Map (val row))))
		result))
    final-result))

(defn raw-live-names []
  (println "Raw-live-names")
  (let [result (reduce (fn [result a-name]
	    (conj result (HashMap. #^java.util.Map (keyworded-names-as-string a-name))))
	  [] 
	  (names-where (fn [_] true)))]
    (println result)
    (java.util.ArrayList. #^java.util.Collection result)))
 
(defn raw-names [from to]
  (let [start (System/currentTimeMillis)
	res (java.util.ArrayList. #^java.util.Collection (reduce (fn [result a-name]
					 (conj result (HashMap. #^java.util.Map (keyworded-names-as-string a-name))))
				       [] 
				       (names-where from to)))]
    ;(println (- (System/currentTimeMillis) start))
    res))
  
(def remote (atom nil))
(def exported (proxy [ServerInterface] []
			 (rawData [from# to# names#] 
				  (raw-data from# to# names#))
			 (rawLiveData [ i#] 
				      (raw-live-data i#))
			 (rawLiveNames []
				       (raw-live-names))
			 (rawNames [from# to#]
				   (raw-names from# to#))
;			 (classData [name#]
;				    (byte-code-for name#))
			 (ping [])
			 ))

(defmacro serve-clients [port transfer-port stop-fn & form]
  `(try
      
      (let [obj# (UnicastRemoteObject/exportObject 
		       exported ~port)]
	(swap! remote (fn [_#] obj#))
	(if (nil? @remote)
	  (throw (IllegalArgumentException. "Got a null as exported" ))))
      
      (let [server-socket# (ServerSocket. ~transfer-port)]
	(. server-socket# setSoTimeout 1000)
	(.start (Thread. (fn []
			   (with-open [ss# server-socket#]
			   (while (not (~stop-fn))
				  (try
				   (with-open [sock# (.accept ss#)
					       output# (ObjectOutputStream. (.getOutputStream sock#))]
				     (. output# writeObject @remote))
				   (catch SocketTimeoutException _#)))))))
	(do ~@form))
      (catch Exception e#
	   (println e#) )
      (finally (when @remote
		 (try 
		  (println "Removeing interface!!!!!!!!!!!!!!!!!")
		  (UnicastRemoteObject/unexportObject @remote true)
		  (catch NoSuchObjectException _# ))
		 (swap! remote (fn [_#] nil))))))

;(deftest simple
;  (is (not (nil? (byte-code-for "clojure.lang.PersistentArrayMap")))))


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
     (is (= (raw-live-data [{"host" "Arne"}]) 
	    {{"host" "Arne" "counter" "Nisse"} { (dparse "20100101 100001") 1  
						 (dparse "20100101 100003") 3}
	     {"host" "Arne" "counter" "Olle"} { (dparse "20100101 100002") 2}})) 
     (is (= (raw-live-data [{"host" "Arne" "counter" "Olle"}]) 
	    {{"host" "Arne" "counter" "Olle"} { (dparse "20100101 100002") 2}})) 
     (is (= (raw-live-data [{"counter" "Olle"}]) 
	    {{"host" "Arne" "counter" "Olle"} { (dparse "20100101 100002") 2}
	     {"host" "Bengt" "counter" "Olle"} {(dparse "20100101 100004") 4}})) 
     (is (= (raw-live-data [{"host" "Nisse"}]) {}))
     (is (= (set (raw-live-names)) #{{"host" "Arne" "counter" "Olle"} 
			       {"host" "Arne" "counter" "Nisse"}
			       {"host" "Bengt" "counter" "Olle"}}))
     (is (empty? 
	  (raw-data (dparse "20070101 010101") 
		    (dparse "20080101 010101") 
		    ["host" "Arne" "counter" "Nisse"]))))))

(deftest test-raw-persistent
 (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
       dparse #(. df parse %)
       tmp (make-temp-dir)
       stop-signal (atom false)
       stop #(deref stop-signal)]

    (try
     (using-history tmp
		    (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100001") 1)
		    (add-data {:host "Arne" :counter "Olle"} (dparse "20100101 100002") 2)
		    (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100003") 3)
		    (add-data {:host "Bengt" :counter "Olle"} (dparse "20100101 100006") 4)
		    (serve-clients 
		     2424 
		     2425 
		     stop 
		     (with-open [s (Socket. (InetAddress/getLocalHost) 2425)
				 ois (ObjectInputStream. (.getInputStream s))]
		       (let [server (.readObject ois)]
			 (is (=  (set (. server rawNames 
					 (dparse "20100101 000000") 
					 (dparse "20100101 100004")))
				 #{{"host" "Arne" "counter" "Nisse"} 
				   {"host" "Arne" "counter" "Olle"}
				   {"host" "Bengt" "counter" "Olle"} ;Fix this should be here, since filtering should occur
				   }))
			 (is (= (. server rawData 
				   (dparse "20100101 000000") 
				    (dparse "20100101 100004")
				   (java.util.ArrayList. ["host" "Arne"]))
				{ {"host" "Arne" "counter" "Nisse"}
				 {(dparse "20100101 100001") 1.0  (dparse "20100101 100003") 3.0}
				 {"host" "Arne" "counter" "Olle"} 
				  {(dparse "20100101 100002") 2.0}}))))))
     
     (finally (rmdir-recursive tmp)
	      (swap! stop-signal (fn [_] true))
	      (Thread/sleep 1200)
	      (clean-live-data-older-than (dparse "19700101 010101"))))))

(deftest test-raw-live-data
  (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (using-live
     (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100001") 1)
     (add-data {:host "Arne" :counter "Olle"} (dparse "20100101 100002") 2)
     (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100003") 3)
     (add-data {:host "Bengt" :counter "Olle"} (dparse "20100101 100004") 4)

     (let [res (raw-live-data [(HashMap. {:host "Arne"})])]
       (is (instance? HashMap res))
       (is (instance? HashMap (key (first res))))
       (is (instance? TreeMap (val (first res)))) 
  ))))

(deftest test-raw-live-names
  (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (using-live
     (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100001") 1)
     (add-data {:host "Arne" :counter "Olle"} (dparse "20100101 100002") 2)
     (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100003") 3)
     (add-data {:host "Bengt" :counter "Olle"} (dparse "20100101 100004") 4)

     (let [res (raw-live-names )]
       (is (instance? java.util.ArrayList res))
       (is (instance? HashMap  (first res)))
 ;      (is (instance? TreeMap (val (first res)))) 
  ))))

  (deftest test-serving
    (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	  dparse #(. df parse %)
      stop-signal (atom false)
      stop #(deref stop-signal)]
    (try
     (clean-live-data-older-than (dparse "19700101 010101"))
     (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100001") 1)
     (add-data {:host "Arne" :counter "Olle"} (dparse "20100101 100002") 2)
     (add-data {:host "Arne" :counter "Nisse"} (dparse "20100101 100003") 3)
     (add-data {:host "Bengt" :counter "Olle"} (dparse "20100101 100004") 4)
     (serve-clients 2424 2425 stop 
		    (with-open [s (Socket. (InetAddress/getLocalHost) 2425)
				ois (ObjectInputStream. (.getInputStream s))]
		      (let [server (.readObject ois)]
			(is (= (set (. server rawLiveNames))
			       #{{"host" "Arne" "counter" "Nisse"} 
				 {"host" "Arne" "counter" "Olle"} 
				 {"host" "Bengt" "counter" "Olle"}}))

			(is (= (. server rawLiveData (HashMap. {"host" "Arne"}))
			       {{"host" "Arne" "counter" "Nisse"} 
				{(dparse "20100101 100001") 1 (dparse "20100101 100003") 3 }
				{"host" "Arne" "counter" "Olle"} 
				{(dparse "20100101 100002") 2}})))))

     (finally (swap! stop-signal (fn [_] true))
	      (Thread/sleep 1200)
	      (clean-live-data-older-than (dparse "20110101 010001")))
)))




