(ns monitor.jmxremote
  (:import (java.io Writer BufferedWriter PrintWriter Reader PushbackReader OutputStreamWriter
		    IOException InputStreamReader InputStream Closeable)
	   (java.net ServerSocket SocketTimeoutException Socket)
	   (java.util.concurrent Executors)
	   (java.util Date))
	  
  (:use [monitor.database :only [add-data]])
  (:use [monitor.server :only [serve tasks-to-start]])
  (:use (monitor termination shutdown tools))
  
  (:use [monitor.jmxcollection :only [jmx-java6]])
   (:use [clojure.contrib.logging :only [info]]))


(def readers-writers (atom #{}))

(defn write-data
  ([data #^Date timestamp value save?]
     (let [to-write (pr-str (into {:data data :time (.getTime timestamp) :value value} (when (not (nil? save?))

											 {:save save?})))]
       (doseq [reader-writer @readers-writers]
	 (try
	   (locking (second reader-writer)
	     (.write #^Writer (second reader-writer) to-write 0 (count to-write))
;	     (.write (second reader-writer) " " 0 1)
					;	     (.flush (second reader-writer))
	     )
	   
	   (catch Exception e
	     (info "Closing writer")
	     (try
	       (.close #^Closeable (second reader-writer))
	       (catch Exception _))
	     (swap! readers-writers (fn [readers-writers] (disj readers-writers reader-writer))))))))
  ([data timestamp value ]
     (write-data data timestamp value nil)))

(defn add-data-from [data-read]
  (if (not (nil? (:save data-read)))
    (do
      (add-data (:data data-read) (Date. #^Long (:time data-read)) (:value data-read) (:save data-read)))
    (do
      (add-data (:data data-read) (Date. #^Long (:time data-read)) (:value data-read)))))


(defn jmx-remote [^String host ^Integer port term?]
  (try
  (let [s (doto (Socket. host port)
	    )
	reader (PushbackReader. (InputStreamReader. (.getInputStream s)))]
    (try
      (while (not (term?))
	(add-data-from (read reader)))
    (catch Exception e
      (if (causes e IOException)
	(throw (IOException. (str "Lost contact with remote jmx at " host ":" port) e)))
      (throw e))))
  (catch Exception e
    (info (str "Can't establish remote jmx connection to " host ":" (str port ". ") (.getMessage e )))
    )))

(defn config-from-text [s]
  (let [input (.replaceAll s "(?m);.*" "")
	tokens (.split #"\s+" (.replaceAll input "[\r\n]+" " "))]

    (println "That would be:" (apply str (butlast (interleave tokens (repeatedly (constantly " "))))))
    tokens
  ))

(defn jmx-probe [args]
  (if (= 1 (count args))
    (jmx-probe (config-from-text (slurp (first args))))
    (do
    (if (System/getProperty "java.awt.headless")
      (shutdown-file ".shutdownjmxprobe")
      (shutdown-button "Monitor JMX Agent"))
    (shutdown-jmx "monitor.jmx")
    (try
      (let [server (ServerSocket. (Integer/parseInt (first args)))
	     accept-executor (Executors/newSingleThreadExecutor
			      (proxy [java.util.concurrent.ThreadFactory] []
				(newThread [^Runnable runnable] (doto (Thread. runnable)
								  (.setDaemon true)))))
	     read-executor (Executors/newSingleThreadExecutor
			    (proxy [java.util.concurrent.ThreadFactory] []
			      (newThread [^Runnable runnable] (doto (Thread. runnable)
								(.setDaemon true)))))]
	 
	 (. read-executor execute (fn readall []
				    (while (not (terminating?))
				      (term-sleep 1)
					;				   (println "#" (count @readers-writers))
				      (doseq [reader-writer @readers-writers]
					(try
					  (dorun (take-while #(<= 0 %) (repeatedly (fn []
										     (let [n (.read ^InputStream (first reader-writer))]
										       n)))))
					  (try
					    (.close ^Closeable (first reader-writer))
					    (catch Exception _))
					  (swap! readers-writers (fn [readers-writers] (disj readers-writers reader-writer)))
					  (catch Exception e
					    (cond
					     (causes e SocketTimeoutException)
					     (do)
					     (causes e IOException)
					     (do
					       (info "Closing reader")
					       (try
						 (.close ^Closeable (first reader-writer))
						 (catch Exception _))
					       (swap! readers-writers (fn [rw] (disj rw reader-writer))))
					     
					     :else (.printStackTrace e))))))))
	 
	 (. accept-executor execute (fn acceptor []
				      (.setSoTimeout server 1000)
				      (while (not (terminating?))
					(try
					  (if-let [socket (.accept server)]
					    (do (info "Got connection")
						(.setSoTimeout socket 1000)
						(swap! readers-writers (fn [rw] (conj rw 
										      [(.getInputStream socket)
										       (BufferedWriter.
											(OutputStreamWriter.
											 (.getOutputStream socket)))])))
						))
					  (catch SocketTimeoutException _)))))
	 (.shutdown accept-executor)
	 (.shutdown read-executor)
	 (let [confs (map (fn [d]
			    {:name (first d)
			     :port (Integer/parseInt (second d))})
			  (partition 2 (next args)))]
	   (dosync (doseq [conf confs]
		     (alter tasks-to-start conj (fn java6-local []
						  (binding [monitor.jmxcollection/*add* write-data]
						    (try
						      (jmx-java6 (:name conf) "localhost" (:port conf) (fn [] (term-sleep 15)))
						      (catch Exception e
							(if (causes e java.rmi.ConnectException)
							  (throw (IOException. (str "Connection to " (:name conf) " lost")))
							  (throw e))
							))))))))
	 (serve))
       (finally (reset! readers-writers #{})))))) ;close them as well