(ns monitor.perfmonservice
  (:use (monitor database perfmon names) )
  (:use [clojure.java.io :only (reader)] )
  (:use [clojure.contrib.logging :only (error info)] )
  (:import (java.net Socket) (java.io PrintWriter))
  (:use (clojure stacktrace)))

(defn- create-obj [line perfmon-names current-time]
  (parse-perfmon-string line 
			(fn onName [identifier name] 
			  (store-name perfmon-names identifier name perfmon-columns))
			(fn onData [identifier value] 
			  (when @current-time 
			    (add-data (get-name perfmon-names identifier) 
					   @current-time 
					   value)))
			(fn onComment [a-comment] 
			  (when @current-time 
			    (add-comment @current-time a-comment)))
			(fn onTime [a] (swap! current-time (fn [_] a )))
			))



(defn add-perfmon-from 
  ([source info-string stop-signal]
     (let [names (ref {})
	   current-time (atom nil)]
       (add-comment (str "Starts " info-string)) 
       (with-open [input #^java.io.Closeable (reader source)] 
	 (loop [data (line-seq input)] 
	   (if  (stop-signal)
	     (add-comment (str "Stops " info-string))
	     (do (create-obj (first data) names current-time)
		 (when-let [more (next data)]
		   (recur more))))
	   ))))
  ([source]
     (add-perfmon-from comments source (atom (fn [] false)))))


(defn perfmon-connection 
"Returns a closure that will try to open a TCP connection to address and port and to a perfmon server and collect perfmon data from it. The stop-signl is fn returning true value when connection should be closed, and closure return."
;TODO make arities that sets filters
 ([adress port stop-signal]

  #(try
    (with-open [socket (Socket. adress port)
		output (PrintWriter. (. socket getOutputStream) true)
		input (. socket getInputStream)]
      (. output println "start")
      (add-perfmon-from input (str "perfmon " adress ":" port) stop-signal))
    (catch Exception e
      (cond 
       (instance? java.net.ConnectException (root-cause e))
       (info (str "Nobody is listening on " adress ":" port))
       (instance? java.net.UnknownHostException (root-cause e))
       (error (str "Unknown host " adress))
	true (throw e)))))
 ([adress port stop-signal hosts-exp categories-exp counters-exp instances-exp]
    #(try
    (with-open [socket (Socket. adress port)
		output (PrintWriter. (. socket getOutputStream) true)
		input (. socket getInputStream)]
      (. output println (str "hosts " hosts-exp))
      (. output println (str "categories " categories-exp))
      (. output println (str "counters " counters-exp))
      (. output println (str "instances " instances-exp))
      (. output println "start")
      (add-perfmon-from input (str "perfmon " adress ":" port " (hosts:\"" hosts-exp "\" categories:\"" categories-exp "\" counters:\"" counters-exp "\" instances:\"" instances-exp "\")") stop-signal))
    (catch Exception e
      (cond 
       (instance? java.net.ConnectException (root-cause e))
       (info (str "Nobody is listening on " adress ":" port))
       (instance? java.net.UnknownHostException (root-cause e))
       (error (str "Unknown host " adress))
	true (throw e))))))


