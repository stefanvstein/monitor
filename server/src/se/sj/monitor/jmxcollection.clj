(ns se.sj.monitor.jmxcollection
  (:use (se.sj.monitor mem jmx) )
  (:import (java.util.concurrent TimeUnit)
  (javax.management JMX ObjectName)  (java.lang.management GarbageCollectorMXBean))
	   
					; for testing purposes
  (:use clojure.test)
					;for repl purposes
  (:use (clojure stacktrace inspector)))


(defn- object-names [server]
  (. server queryNames nil nil))

(defn- collector-names-and-beans 
  [server]
  (reduce #(if-let [match  
		      (re-matches #"java.lang:type=GarbageCollector,name=(.*)" (. %2 toString))] 
	     (assoc %1 
	       (second match) 
	       (. JMX newMXBeanProxy server %2 GarbageCollectorMXBean))
	     %1) 
	  {}
	  (object-names server)))

(defn- collector-stat
  [collector-name collectorBean]
  (let [hostname (remote-hostname) 
	vm (vmname)
	collectorname (. collectorBean getName)]
    (list [{:host hostname 
	    :jvm vm 
	    :category "GarbageCollector" 
	    :counter "Count" 
	    :instance collectorname} 
	   (. collectorBean getCollectionCount)]
	  [{:host hostname 
	    :jvm vm 
	    :category "GarbageCollector" 
	    :counter "Time" 
	    :instance collectorname} 
	   (. collectorBean getCollectionTime)])))

 
(defn std
  [database comments]
  (fn [server]
    (let [collectors (collector-names-and-beans server)]
      (let [the-time (remote-time)]

	(dosync 
	 (alter database add 
		{:host (remote-hostname) 
		 :jvm (vmname) 
		 :category "Runtime" 
		 :counter "UpTime"} 
		the-time 
		(remote-uptime *current-runtime-bean* TimeUnit/SECONDS))
	 (dorun (map #(let [data (collector-stat % (get collectors %))
			    count-data (first data) 
			    time-data (second data)]
			(alter database add (first count-data) the-time (second count-data))
			(alter database add (first time-data) the-time (second time-data)))
		     (keys collectors))))))))


  
  


