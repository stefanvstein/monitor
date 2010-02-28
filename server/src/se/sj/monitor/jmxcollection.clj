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

(defn- collector-beans 
  [server comments]
  (reduce #(if-let [match  
		      (re-matches #"java.lang:type=GarbageCollector,name=.*" (. %2 toString))] 
	     (let [bean (. JMX newMXBeanProxy server %2 GarbageCollectorMXBean)
		   timestamp (remote-time)
		   comment   (with-out-str 
				 (print (str "Garbage Collector \"" (. bean getName) "\" collecting "))
				 (dorun (map (fn [a] (print (str a ", "))) 
					     (seq (. bean getMemoryPoolNames)))) 
				 (print (str "was found in " (vmname) " at " (remote-hostname))))]
	       (dosync (alter comments assoc timestamp comment))
	       (conj %1 bean))
	     %1) 
	  []
	  (object-names server)))

(defn- collector-stat
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

 
(defn std
"Returns a fn to that collects java 6 metrics and store them to database, assuming one is set"
  [comments]
  (fn [server]
    (let [collectors (collector-beans server comments)]
      (let [the-time (remote-time)]

	  (add-data 
	   {:host (remote-hostname) 
	    :jvm (vmname) 
	    :category "Runtime" 
	    :counter "UpTime"}
	   the-time (remote-uptime *current-runtime-bean* TimeUnit/SECONDS))
	  
		
	 (dorun (map #(let [data (collector-stat %)
			    count-data (first data) 
			    time-data (second data)]
			(add-data (first count-data) the-time (second count-data))
			(add-data (first time-data) the-time (second time-data)))
		     (seq collectors)))))))

(deftest test-local-std
  (if-let [jmxport (Integer/parseInt (System/getProperty "com.sun.management.jmxremote.port"))]
    (using-live 
     (let [comments (ref {})
	   stdfn (std comments)]
       (using-jmx 
	jmxport 
	(fn [server]
	  (stdfn server)
	  (stdfn server)

	  (println (data-by (fn [_] true)))
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
	    (is (<= (first uptime-values) (second uptime-values))))))))
    (is nil "No com.sun.management.jmxremote.port")))

  
  


