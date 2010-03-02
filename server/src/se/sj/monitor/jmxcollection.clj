(ns se.sj.monitor.jmxcollection
  (:use (se.sj.monitor database jmx) )
  (:import (java.util.concurrent TimeUnit)
	   (javax.management JMX ObjectName)  
	   (java.lang.management GarbageCollectorMXBean))
	   
					; for testing purposes
  (:use clojure.test)
					;for repl purposes
  (:use (clojure stacktrace inspector)))


(defn- object-names [server]
  (. server queryNames nil nil))

(defn collector-beans 
  ([server comments]
     (reduce #(if-let [match  
		       (re-matches #"java.lang:type=GarbageCollector,name=.*" (. %2 toString))] 
		(let [bean (. JMX newMXBeanProxy server %2 GarbageCollectorMXBean)
		      timestamp (remote-time)
		      comment (with-out-str 
				(print (str "Garbage Collector \"" (. bean getName) "\" collecting "))
				(dorun (map (fn [a] (print (str a ", "))) 
					    (seq (. bean getMemoryPoolNames)))) 
				(print (str "was found in " (vmname) " at " (remote-hostname))))]
		  (dosync (alter comments assoc timestamp comment))
		  (conj %1 bean))
		%1) 
	     []
	     (object-names server)))
([server]
   (let [comments (ref {})
	 result (collector-beans server comments)]
     (dorun (map (fn [comment] (println (val comment))) @comments))
     result)))

(defn collector-data
  [collectorBean]
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


(defn- jmx-java6-impl
  [stop-fn]
    (fn [server]
      (let [col-beans (collector-beans server)]
	(while (not (stop-fn))
	       (let [the-time (remote-time)]
		 (dorun (map #(let [data (collector-data %)
				    count-data (first data) 
				    time-data (second data)]
				(add-data (first count-data) the-time (second count-data))
				(add-data (first time-data) the-time (second time-data)))
			     (seq col-beans)))
		 (add-data 
		  {:host (remote-hostname) 
		   :jvm (vmname) 
		   :category "Runtime" 
		   :counter "UpTime"}
		  the-time (remote-uptime *current-runtime-bean* TimeUnit/SECONDS)))))))

(defn jmx-java6 
 ([port stop-fn]
     (using-jmx port (jmx-java6-impl stop-fn)))
 ([host port stop-fn]
   (using-jmx host port (jmx-java6-impl stop-fn)))
 ([stop-fn] 
    (if-let [jmxport (Integer/parseInt (System/getProperty "com.sun.management.jmxremote.port"))]
      (jmx-java6 jmxport stop-fn)
      (throw (IllegalArgumentException. "com.sun.management.jmxremote.port not set on local process"))
    )))
 
(deftest test-java6
 (using-live
  (let [fun (fn [] (let [countdown (atom 2)]
		     (jmx-java6 (fn [] (swap! countdown dec)
				  (= -1 @countdown)))))]
    (fun)
    (let [collector-time (data-by (fn [i] 
				    (and (= (:category i) "GarbageCollector") 
					 (= (:counter i) "Time"))))
	  time-values (vals (val (first collector-time)))]
      (is (= 2 (count time-values)))
      (is (<= (first time-values) (second time-values))))
    (let [collector-count (data-by (fn [i] 
				     (and (= (:category i) "GarbageCollector") 
					  (= (:counter i) "Count"))))
	  count-values (vals (val (first collector-count)))]
      (is (= 2 (count count-values)))
      (is (<= (first count-values) (second count-values))))
    (let [uptime (data-by (fn [i] (= (:counter i) "UpTime"))) 
	  uptime-values (vals (val (first uptime)))]
      (is (= 2 (count uptime-values)))
      (is (<= (first uptime-values) (second uptime-values))))
    

)))


  
  


