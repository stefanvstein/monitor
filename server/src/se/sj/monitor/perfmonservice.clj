(ns se.sj.monitor.perfmonservice
  (:use (se.sj.monitor database perfmon names) )
  (:use [clojure.contrib.duck-streams :only (reader)] )
  (:import (java.net Socket) (java.io PrintWriter))
  ; for testing purposes
  (:use clojure.test)
  ;for repl purposes
  (:use (clojure stacktrace inspector)))
;This should be private to a perfmon connection. It translates names for a connection


(def comments (atom {}))

(defn causes [#^java.lang.Throwable e] (take-while #(not (nil? %)) (iterate #(when % (. % getCause)) e)))

(defn- create-obj [line perfmon-names current-time comments]
  (parse-perfmon-string line 
			(fn [identifier name] 
			  (store-name perfmon-names identifier name perfmon-columns))
			(fn [identifier value] 
			  (when @current-time 
					;(dosync (alter perfmon-data add 
					;		   (get-name perfmon-names identifier) 
					;		   @current-time 
					;		   value))))
			    (add-data (get-name perfmon-names identifier) 
					   @current-time 
					   value)))
			(fn [a-comment] 
			  (when @current-time 
			    (swap! comments  assoc @current-time 
				   (if-let [comments (get @comments @current-time)] 
				     (conj comments a-comment)
				     (list a-comment)))))
			(fn [a] (swap! current-time (fn [_] a )))
			))


(defn stop 
  "Stop signal is expected to be a atom of boolean fn []"
  [stop-signal]
  (swap! stop-signal (fn [_] (fn [] true))))

(defn add-perfmon-from 
  ([comments source stop-signal]
     (let [names (ref {})
	   current-time (atom nil)]
       (with-open [input (reader source)] 
	 (loop [data (line-seq input)] 
	   (when (not (@stop-signal))
	     (create-obj (first data) names current-time comments)
	     (when-let [more (next data)]
	       (recur more)))))))
  ([comments source]
     (add-perfmon-from comments source (atom (fn [] false)))))


(defn perfmon-connection 
"Returns a closure that will try to open a TCP connection to address and port and to a perfmon server and collect perfmon data from it. The stop-signl is expected to be atom of a bool fn. the clojure will return when stop-signal is false"
;TODO make arities that sets filters
 ([comments adress port stop-signal]

  #(with-open [socket (Socket. adress port)
	      output (PrintWriter. (. socket getOutputStream) true)
	      input (. socket getInputStream)]
     (. output println "start")
     (add-perfmon-from comments input stop-signal)))
 ([comments adress port stop-signal hosts-exp categories-exp counters-exp instances-exp]
  #(with-open [socket (Socket. adress port)
	      output (PrintWriter. (. socket getOutputStream) true)
	      input (. socket getInputStream)]
     (. output println (str "hosts " hosts-exp))
     (. output println (str "categories " categories-exp))
     (. output println (str "counters " counters-exp))
     (. output println (str "instances " instances-exp))
     (. output println "start")
     (add-perfmon-from comments input stop-signal))))

(defn start-collecting-perfmon-in-tread
  "Starts collecting and returns a stopper"
  [comments adress port]
  (let [stopper (atom (fn [] false))
	f (perfmon-connection  comments adress port stopper)
	thread (Thread. f (str "Perfmon collector " adress ":" port))]
    (. thread start )
    stopper))
