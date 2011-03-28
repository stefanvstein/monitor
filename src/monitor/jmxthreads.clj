(ns monitor.jmxthreads
  (:use [monitor.jmxcollection :only [*add* jmx-threads]])
  (:use [monitor.termination :only [term-sleep terminating?]])
  (:use [monitor.shutdown])
  (:import (java.io PrintWriter FileWriter FileReader PushbackReader))
  (:import (java.util Date))
  )

(defn- as-int [i]
  (if (instance? String i)
    (Integer/valueOf i)
    i))

(defn- connect-remote [client-port client-host]
  (let [port  (as-int client-port)]
    (with-open [s (java.net.Socket. client-host  port)
		ois (java.io.ObjectInputStream. (.getInputStream s))]
      (.readObject ois))))

(defn- named-map [m]
  (into {}
	(map (fn [n] [(name (key n)) (val n)])
	     m)))

(defn read-threads-to-file
  ([] (throw  (IllegalArgumentException. "Supply name jmx-host jmx-port file seconds-per-sample as arguments")))
  ([_] (read-threads-to-file))
  ([_ _] (read-threads-to-file))
  ([_ _ _] (read-threads-to-file))
  ([_ _ _ _] (read-threads-to-file))
  ([_ _ _ _ _ & _] (read-threads-to-file))
  ([name jmx-host jmx-port file seconds-per-sample]
     (let [port (if (instance? String jmx-port)
		  (Integer/valueOf jmx-port)
		  jmx-port)
	   seconds (if (instance? String seconds-per-sample)
		  (Integer/valueOf seconds-per-sample)
		  seconds-per-sample)]
     (with-open [out (PrintWriter. (FileWriter. file) true)]
       (binding [*add* (fn [names ^Date time-stamp value]
			 (.println out (pr-str [names (.getTime time-stamp) value])))]
	 (jmx-threads name jmx-host port (fn [] (term-sleep seconds))))))))

(defn- read-and-add-as-client [name jmx-host jmx-port client-host client-port]
  (let [connection (connect-remote client-port client-host)
	data (atom {})
	stop-fn  (fn []
		   (let [d @data]
		     (reset! data {})
		     (.addWide connection d))
		   (term-sleep 15))
	
	   ]
       (binding [*add* (fn [names ^Date time-stamp value]
			 (swap! data assoc names {time-stamp value}) 
			 #_(.add connection (named-map names) {time-stamp value}))]
	 (jmx-threads name jmx-host (as-int jmx-port) stop-fn))))
  

(defn read-threads
  ([] (throw (IllegalArgumentException. "Supply name jmx-host jmx-port server-host server-port as arguments")))
  ([_] (read-threads))
  ([_ _] (read-threads))
  ([_ _ _] (read-threads))
  ([_ _ _ _] (read-threads))
  ([_ _ _ _ _ & _] (read-threads))
  ([name jmx-host jmx-port client-host client-port]
       (if (System/getProperty "java.awt.headless")
	 (shutdown-file ".shutdownthreadsprobe")
	 (shutdown-button "Monitor Threads Agent"))
       (shutdown-jmx "monitor.threads")
       (read-and-add-as-client name jmx-host jmx-port client-host client-port)
       
))

    

(defn import-threads-file
  ([] (throw (IllegalArgumentException. "Supply file server-host server-port as arguments")))
  ([_] (import-threads-file))
  ([_ _] (import-threads-file))
  ([_ _ _ & _] (import-threads-file))
  ([file client-host client-port]
     (let [connection (connect-remote client-port client-host)]
     (with-open [input (PushbackReader. (FileReader. file))]
       (let [data (reduce (fn [r line]
			    (update-in r [(named-map (first line))] 
				       #(assoc (if % % {}) (Date. ^Long (second line)) (nth line 2))))
			  {}
			  (take-while #(not (nil? %)) (repeatedly (fn [] (read input false nil)))))]
	      
	     (doseq [d data]
	       (.add connection (key d) (val d))))))))

     
  