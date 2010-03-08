(ns se.sj.monitor.jmxcollection
  (:use (se.sj.monitor database jmx) )
  (:use (clojure.contrib repl-utils))
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
  [server] ;sun.management.GarbageCollectorImpl
  (reduce #(if-let [match  
		    (re-matches #"java.lang:type=GarbageCollector,name=.*" (. %2 toString))] 
	     (let [bean (. JMX newMXBeanProxy server %2 GarbageCollectorMXBean)
		   timestamp (remote-time)]

	       (add-comment timestamp (with-out-str 
					(print (str "Garbage Collector \"" (. bean getName) "\" collecting "))
					(dorun (map (fn [a] (print (str a ", "))) 
						    (seq (. bean getMemoryPoolNames)))) 
					(print (str "was found in " (vmname) " at " (remote-hostname)))))
	       (conj %1 bean))
		  %1) 
	     []
	     (object-names server)))

(defn collector-data
  [collectorBean]
  (let [hostname (remote-hostname) 
	vm (vmname)
	collectorname (. collectorBean getName)
	hasLastGc (some #(= "getLastGcInfo" (.getName %)) (.getMethods (class collectorBean)))]
;(println (seq (.getMethods (class collectorBean))))
    (concat (list [{:host hostname 
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
	   (. collectorBean getCollectionTime)])
	    (if hasLastGc
	      (let [gcInfo (. collectorBean getLastGcInfo)
		    last-gc-started (.getStartTime gcInfo)
		    last-gc-duration (.getDuration gcInfo)
		    beforeGc (.getMemoryUsageBeforeGc gcInfo)
		    afterGc (.getMemoryUsageBeforeGc gcInfo)]
		(list [{:host hostname
			:jvm vm
			:category "GarbageCollector"
			:counter "LastCollectionStarted"
			:instance collectorname}
		       last-gc-started]
		      [{:host hostname
			:jvm vm
			:category "GarbageCollector"
			:counter "LastCollectionDuration"
			:instance collectorname}
		       last-gc-duration]
		      [{:host hostname
			:jvm vm
			:category "GarbageCollector"
			:counter "UsedBeforeGc"
			:instance collectorname}
		       (get beforeGc "used")]
		       [{:host hostname
			:jvm vm
			:category "GarbageCollector"
			:counter "UsedAfterGc"
			:instance collectorname}
		       (get afterGc "used")]))(list)))))

(defn collector-keys [server]
  (reduce (fn [result object-name] 
	    (if (re-matches #"java.lang:type=GarbageCollector,name=.*" 
				       (. object-name toString))
	      (let [attributes (seq (. server getAttributes 
				   object-name (into-array ["Name" 
							    "CollectionCount" 
							    "CollectionTime" 
							    "LastGcInfo"])))
		    fns (reduce (fn [result attribut]
				  (condp = (.getName attribut)
				    "CollectionCount" (conj result (fn [attributes] 
								     (when-let [the-attribute (some #(= "CollectionCount" (.getName %)) attributes)]
								       (when-let [name (some #(= "Name" (.getName %)) attributes)] 
								       [{:category "GarbageCollector"
									:counter "Count"
									:instance name
									:jvm (vmname)
									:host (remote-hostname)}
									(.getValue the-attribute)]))))

				    "CollectionTime" (conj result (fn [attributes] 
								     (when-let [the-attribute (some #(= "CollectionCount" (.getName %)) attributes)]
								       (when-let [name (some #(= "Name" (.getName %)) attributes)] 
								       [{:category "GarbageCollector"
									:counter "Count"
									:instance name
									:jvm (vmname)
									:host (remote-hostname)}
									(.getValue the-attribute)]))))

				   ; "LastGcInfo" (conj result )
				    result)) [] attributes)]
		
	      (assoc result object-name fns))
	     result))
	  {} (object-names server)))

(defn collector-attributes
  [server]
  (let [obj-names 
	(reduce #(if-let [match  
			  (re-matches #"java.lang:type=GarbageCollector,name=.*" (. %2 toString))]
		   (conj %1 %2)
		   %1)
		[] (object-names server))]
    (dorun (map (fn [object-name]
		  (let [attributes (. server getAttributes 
				      object-name 
				      (into-array ["Name" "CollectionCount" "CollectionTime" "LastGcInfo"]))]
		    (dorun (map #(do 
				   (condp = (.getName %)
				     "CollectionCount" (println (str "Collection count " (.getValue %)))
				     "CollectionTime"  (println (str "Collection time " (.getValue %)))
				     "LastGcInfo"  (try
						    (when-let [gcinfo-class (Class/forName "com.sun.management.GcInfo")]
						      (let [from-method (. gcinfo-class getMethod "from" (into-array [javax.management.openmbean.CompositeData])) 
							    gcinfo (. from-method invoke nil (to-array[(.getValue %)]))]
							
							(println (.getId gcinfo))
							
							(let [after (.getMemoryUsageAfterGc gcinfo)]
							  (dorun (map (fn [i]
									(print (key i))
									(print " ")
									(println (. (val i) getUsed)))
								      after)))))
						    (catch IllegalArgumentException e (println (. e getMessage)))
						    (catch ClassNotFoundException e (println (. e getMessage))))
				     "Name"  (println (str "Name " (.getValue %)))
				     ))
				(seq attributes)))))
		obj-names))))


(defn memory-pool-beans 
  [server]
  (reduce #(if-let [match  
		       (re-matches #"java.lang:type=MemoryPool,name=.*" (. %2 toString))] 
		(let [bean (. JMX newMXBeanProxy server %2 GarbageCollectorMXBean)
		      timestamp (remote-time)]
		  (add-comment timestamp (with-out-str 
					   (print (str "Memory Pool \"" (. bean getName) "\" managed by "))
					   (dorun (map (fn [a] (print (str a ", "))) 
						       (seq (. bean getMemoryManagerNames)))) 
					   (print (str "was found in " (vmname) " at " (remote-hostname)))))
		  %1)) 
	     []
	     (object-names server)))
(comment
(defn memory-pool-data
  [memory-pool-bean]
  (let [hostname (remote-hostname) 
	vm (vmname)
	poolname (. memory-pool-bean getName)]
    (list [{:host hostname 
	    :jvm vm 
	    :category "MemoryPool" 
	    :counter "Count" 
	    :instance poolname} 
	   (. collectorBean getCollectionCount)]
	  [{:host hostname 
	    :jvm vm 
	    :category "GarbageCollector" 
	    :counter "Time" 
	    :instance collectorname} 
	   (. collectorBean getCollectionTime)])))
)
(defn- jmx-java6-impl
  [stop-fn]
    (fn [server]
      ;(collector-attributes server)
     ; (dorun (map #(println %) (collector-keys server)))
;(println "*****")
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
    (if-let [jmxport (System/getProperty "com.sun.management.jmxremote.port")]
      (try
       (jmx-java6 (Integer/parseInt jmxport) stop-fn)
       (catch NumberFormatException e
	 (throw (IllegalArgumentException. (str "com.sun.management.jmxremote.port set to something else ththan integer " jmxport )))))
      (throw (IllegalArgumentException. "com.sun.management.jmxremote.port not set on local process")))))
 
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


  
  


