(ns monitor.clientif
  (:import (java.io ByteArrayOutputStream IOException ObjectOutputStream ObjectInputStream))
  (:import (monitor ServerInterface ServerInterface$Granularity ServerInterface$Transform))
  (:import (java.rmi.server UnicastRemoteObject))  
  (:import (java.rmi RemoteException NoSuchObjectException))
  (:import (java.net ServerSocket Socket InetAddress SocketTimeoutException))
  (:import (java.util HashMap TreeMap))
  (:use (clojure stacktrace test))
  (:use [clojure.contrib import-static])
  (:use (monitor database calculations))
  (:use cupboard.utils)
  (:use [clojure.contrib.logging :only [info error]]))

(import-static java.util.Calendar YEAR MONTH DAY_OF_MONTH HOUR_OF_DAY MINUTE SECOND MILLISECOND)

(defn- names-as-keyworded [names]
  (reduce (fn [result name]
	    (assoc result (keyword (key name)) (val name))) 
	  {} names))

(defn- keyworded-names-as-string [names]
 (let [result (reduce (fn [result a-name]
	    (conj result (name (key a-name)) (val a-name))) 
	  [] names)]
(apply array-map result)))

(defn raw-data [from to transform granularity names]
  (when (odd? (count names))
    (throw (IllegalArgumentException. "Odd number of names")))
  (let [keyed-names (interleave 
		     (map #(keyword (first %)) (partition 2 names)) 
		     (map #(second %) (partition 2 names)))
	result (apply data-by from to keyed-names)
	final-result (HashMap.)]
    (dorun (map (fn [row]
		  (let [gran (condp = granularity
				 ServerInterface$Granularity/SECOND SECOND
				 ServerInterface$Granularity/MINUTE MINUTE
				 ServerInterface$Granularity/HOUR HOUR
				 ServerInterface$Granularity/DAY DAY)
			calculated-values (condp = transform
					      ServerInterface$Transform/RAW (val row)
					      ServerInterface$Transform/AVERAGE_MINUTE 
					      (into (sorted-map) (sliding-average (val row) 1 MINUTE gran))
					      ServerInterface$Transform/AVERAGE_HOUR 
					      (into (sorted-map) (sliding-average (val row) 1 HOUR gran))
					      ServerInterface$Transform/AVERAGE_DAY 
					      (into (sorted-map) (sliding-average (val row) 1 DAY gran))
					      ServerInterface$Transform/MEAN_MINUTE
					      (into (sorted-map) (sliding-mean (val row) 1 MINUTE gran))
					      ServerInterface$Transform/MEAN_HOUR
					      (into (sorted-map) (sliding-mean (val row) 1 HOUR gran))
					      ServerInterface$Transform/MEAN_DAY
					      (into (sorted-map) (sliding-mean (val row) 1 DAY gran))
					      ServerInterface$Transform/MIN_MINUTE
					      (into (sorted-map) (sliding-min (val row) 1 MINUTE gran))
					      ServerInterface$Transform/MIN_HOUR
					      (into (sorted-map) (sliding-min (val row) 1 HOUR gran))
					      ServerInterface$Transform/MIN_DAY
					      (into (sorted-map) (sliding-min (val row) 1 DAY gran))
					      ServerInterface$Transform/MAX_MINUTE
					      (into (sorted-map) (sliding-max (val row) 1 MINUTE gran))
					      ServerInterface$Transform/MAX_HOUR
					      (into (sorted-map) (sliding-max (val row) 1 HOUR gran))
					      ServerInterface$Transform/MAX_DAY
					      (into (sorted-map) (sliding-max (val row) 1 DAY gran))
					      ServerInterface$Transform/PER_SECOND
					      (into (sorted-map) (sliding-per- (val row) gran SECOND))
					      ServerInterface$Transform/PER_MINUTE
					      (into (sorted-map) (sliding-per- (val row) gran MINUTE))
					      ServerInterface$Transform/PER_HOUR
					      (into (sorted-map) (sliding-per- (val row) gran HOUR))
					      ServerInterface$Transform/PER_DAY
					      (into (sorted-map) (sliding-per- (val row) gran DAY))
					      (throw (IllegalArgumentException. (str transform " not yet implemented"))))]
		    (. final-result put (HashMap. #^java.util.Map (keyworded-names-as-string (key row))) (TreeMap. #^java.util.Map calculated-values))))
	
		result))
    final-result))


(defn raw-live-data [names transform]
  (let [keyworded-names (reduce (fn [r i] (conj r (names-as-keyworded i))) [] names)
	result (reduce (fn [r i] (merge r (data-by (fn [a-name] 
			 (every? (fn [requirement] 
				   (= ((key requirement) a-name) 
				      (val requirement))) 
				 i))))) {} keyworded-names)
	final-result (HashMap.)
]
    
    (dorun (map (fn [row]
		  (let [calculated-values (condp = transform
					      ServerInterface$Transform/RAW (val row)
					      ServerInterface$Transform/AVERAGE_MINUTE
					      (into (sorted-map) (sliding-average (val row) 1 MINUTE MINUTE))
					      ServerInterface$Transform/MEAN_MINUTE
					      (into (sorted-map) (sliding-mean (val row) 1 MINUTE MINUTE))
					      ServerInterface$Transform/PER_SECOND
					      (into (sorted-map) (sliding-per- (val row) MINUTE SECOND))
					      ServerInterface$Transform/PER_MINUTE
					      (into (sorted-map) (sliding-per- (val row) MINUTE MINUTE))
					      ServerInterface$Transform/PER_HOUR
					      (into (sorted-map) (sliding-per- (val row) MINUTE HOUR))
					      ServerInterface$Transform/PER_DAY
					      (into (sorted-map) (sliding-per- (val row) MINUTE DAY))
					      (throw (IllegalArgumentException. (str transform " not yet implemented"))))]
		    (. final-result put (HashMap. #^java.util.Map (keyworded-names-as-string (key row))) (TreeMap. #^java.util.Map calculated-values))))
		result))
    final-result))

(defn raw-live-names []
  ;(println "Raw-live-names")
  (let [result (reduce (fn [result a-name]
	    (conj result (HashMap. #^java.util.Map (keyworded-names-as-string a-name))))
	  [] 
	  (names-where (fn [_] true)))]
   ; (println result)
    (java.util.ArrayList. #^java.util.Collection result)))
 
(defn raw-names [from to]
  (let [start (System/currentTimeMillis)
	res (java.util.ArrayList. #^java.util.Collection (reduce (fn [result a-name]
					 (conj result (HashMap. #^java.util.Map (keyworded-names-as-string a-name))))
				       [] 
				       (names-where from to)))]
    ;(println (- (System/currentTimeMillis) start))
    res))
  

(def exported (proxy [ServerInterface] []
		(rawData [from# to# names# transform# granularity#]
			 ;(println "Raw data for " from# " to " to#)
			 (raw-data from# to# transform# granularity# names#))
		(rawLiveData [ i# transform#] 
			     (raw-live-data i# transform#))
		(rawLiveNames []
			      (raw-live-names))
		(rawNames [from# to#]
			  (raw-names from# to#))
		(ping [])
		))

(defmacro serve-clients [port transfer-port terminating? & form]
  `(if-let [obj# (UnicastRemoteObject/exportObject 
		  exported ~port)]
     (try
       (info "Client interface created")
       (let [server-socket# (ServerSocket. ~transfer-port)]
	 (. server-socket# setSoTimeout 1000)
	 (doto (Thread. (fn []
			  (with-open [ss# server-socket#]
			    (info (str "Clients welcome on port " ~transfer-port))
			    (while (not (~terminating?))
			      (try
				(with-open [sock# (.accept ss#)
					    output# (ObjectOutputStream. (.getOutputStream sock#))]
				  (. output# writeObject obj#))
				(catch SocketTimeoutException _#)))
			    (info "Stops serving client requests"))))
	   (.setName "Client acceptor")
	   (.start))
	 (do ~@form))
       (finally
	(info "Removeing client interface")
	(UnicastRemoteObject/unexportObject exported true)))
     (throw (IllegalArgumentException. "Got a null as exported" ))))
   

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
     (is (= (raw-live-data [{"host" "Arne"}] ServerInterface$Transform/RAW) 
	    {{"host" "Arne" "counter" "Nisse"} { (dparse "20100101 100001") 1  
						 (dparse "20100101 100003") 3}
	     {"host" "Arne" "counter" "Olle"} { (dparse "20100101 100002") 2}})) 
     (is (= (raw-live-data [{"host" "Arne" "counter" "Olle"}] ServerInterface$Transform/RAW) 
	    {{"host" "Arne" "counter" "Olle"} { (dparse "20100101 100002") 2}})) 
     (is (= (raw-live-data [{"counter" "Olle"}] ServerInterface$Transform/RAW) 
	    {{"host" "Arne" "counter" "Olle"} { (dparse "20100101 100002") 2}
	     {"host" "Bengt" "counter" "Olle"} {(dparse "20100101 100004") 4}})) 
     (is (= (raw-live-data [{"host" "Nisse"}] ServerInterface$Transform/RAW) {}))
     (is (= (set (raw-live-names)) #{{"host" "Arne" "counter" "Olle"} 
			       {"host" "Arne" "counter" "Nisse"}
			       {"host" "Bengt" "counter" "Olle"}}))
     (is (empty? 
	  (raw-data (dparse "20070101 010101") 
		    (dparse "20080101 010101")
		    ServerInterface$Transform/RAW
		    ServerInterface$Granularity/SECOND
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
				   (java.util.ArrayList. ["host" "Arne"])
				   ServerInterface$Transform/RAW
				   ServerInterface$Granularity/SECOND)
				  
				{ {"host" "Arne" "counter" "Nisse"}
				 {(dparse "20100101 100001") 1.0  (dparse "20100101 100003") 3.0}
				 {"host" "Arne" "counter" "Olle"} 
				 {(dparse "20100101 100002") 2.0}}))
			 (println (. server rawData
				     (dparse "20100101 000000") 
				   (dparse "20100101 100006")
				   (java.util.ArrayList. ["host" "Arne"])
				   ServerInterface$Transform/RAW
				   ServerInterface$Granularity/SECOND))
			 (println (. server rawData
				     (dparse "20100101 000000") 
				   (dparse "20100101 100006")
				   (java.util.ArrayList. ["host" "Arne"])
				   ServerInterface$Transform/AVERAGE_MINUTE
				   ServerInterface$Granularity/MINUTE))))))
     
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




