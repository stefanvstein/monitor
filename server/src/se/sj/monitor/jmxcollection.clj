(ns se.sj.monitor.jmxcollection
  (:use (se.sj.monitor database jmx) )
  (:use (clojure.contrib repl-utils))
  (:import (java.util.concurrent TimeUnit)
	   (javax.management JMX ObjectName)  
	   (java.lang.management MemoryUsage ThreadMXBean))
	   
					; for testing purposes
  (:use clojure.test)
					;for repl purposes
  (:use (clojure stacktrace inspector)))


(defn- object-names [server]
  (. server queryNames nil nil))

(defn collector-keys [server]
  (let [hasGcInfo  (try
		    (let [gcinfo-class (Class/forName "com.sun.management.GcInfo")
			  from-method (. gcinfo-class getMethod "from" (into-array [javax.management.openmbean.CompositeData]))
			  create (fn [at]; (println "create called") 
				   (.invoke from-method nil (to-array [(.getValue at)])))]
		      create)
		    (catch ClassNotFoundException _ nil))]
							    
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
								     (when-let [the-attribute (some #(when (= "CollectionCount" (.getName %))
												       %) attributes)]
								       (when-let [name (some #(when (= "Name" (.getName %)) (.getValue %)) attributes)] 

									 [{:category "GarbageCollector"
									   :counter "Count"
									   :instance name
									   :jvm (vmname)
									   :host (remote-hostname)}
									  (double (.getValue the-attribute))]))))

				    "CollectionTime" (conj result (fn [attributes] 
								     (when-let [the-attribute (some #(when (= "CollectionTime" (.getName %)) 
												       %) attributes)]
								       (when-let [name (some #(when (= "Name" (.getName %)) (.getValue %)) attributes)] 
								       [{:category "GarbageCollector"
									:counter "Time"
									:instance name
									:jvm (vmname)
									:host (remote-hostname)}
									(/ (.getValue the-attribute) 1000.0)]))))

				    "LastGcInfo"  (if hasGcInfo 
						    (conj result (fn [attributes] 
								   (when-let [the-attribute (some #(when (= "LastGcInfo" (.getName %)) 
												     %) attributes)]
								     (when-let [name (some #(when (= "Name" (.getName %)) (.getValue %)) attributes)] 
								       (when-let [gcinfo (hasGcInfo the-attribute)]
									 (let [before-gc (.getMemoryUsageBeforeGc gcinfo)
									       after-gc (.getMemoryUsageAfterGc gcinfo)
									       result [{:category "GarbageCollector"
											:counter "Last Duration"
											:section name
											:jvm (vmname)
											:host (remote-hostname)}
										       (/ (.getDuration gcinfo) 1000.0)]
									       usage-before (reduce (fn [result area]
					;		  (println (str "Before " (key area) " " name " " (.getUsed (val area))))
												      (conj result {:category "GarbageCollector"
														    :counter "Usage Before Last"
														    :instance (key area)
														    :jvm (vmname)
														    :host (remote-hostname)
														    :section name}
													    (double (.getUsed (val area))))) [] before-gc)
									       committed-after (reduce (fn [result area]
													 (conj result {:category "GarbageCollector"
														       :counter "Committed After Last"
														       :instance (key area)
														       :jvm (vmname)
														       :host (remote-hostname)
														       :section name}
													       (/ (.getCommitted (val area)) 1.0))) [] after-gc)
									       max-after (reduce (fn [result area]
												   (conj result {:category "GarbageCollector"
														 :counter "Max After Last"
														 :instance (key area)
														 :jvm (vmname)
														 :host (remote-hostname)
														 :section name}
													 (double (.getMax (val area))))) [] after-gc)
									       
									       usage-after (reduce (fn [result area]
												     (conj result {:category "GarbageCollector"
														   :counter "Usage After Last"
														   :instance (key area)
														   :jvm (vmname)
														   :host (remote-hostname)
														   :section name}
													   (double (.getUsed (val area))))) [] after-gc)]
									   (concat result usage-before usage-after committed-after max-after))
									 )))))
						    result)
				    result)) [] attributes)]
		(assoc result object-name fns))
	      result))
	  {} (object-names server))))

(defn collector-values [collector-keys server]
  (reduce (fn [result a-key] 
	    (let [attributeList (.asList (.getAttributes server (key a-key) (into-array ["Name" 
							    "CollectionCount" 
							    "CollectionTime" 
							    "LastGcInfo"])))]
	      (reduce (fn [re aval]
			(let [r (aval attributeList)]
			(apply assoc re r))) 
		      result (val a-key)))) 
	  {} collector-keys))

(defn memory-fns [server]
  (reduce (fn [r object-name] 
	    (if (re-matches #"java.lang:type=MemoryPool,name=.*" (. object-name toString))
	      (let [attributes (seq (. server getAttributes 
				       object-name (into-array ["Usage" 
								"Name"])))
		    fns (reduce (fn [result attribut]
				  (condp = (.getName attribut)
				    "Usage" (conj result (fn [attributes] 
							   (when-let [the-attribute (some #(when (= "Usage" (.getName %))
											     %) attributes)]
							     (when-let [name (some #(when (= "Name" (.getName %)) (.getValue %)) attributes)]
							       ;(println (str "Use " name " " (.getUsed (MemoryUsage/from (.getValue the-attribute)))))
							       [{:category "Memory"
								 :counter "Usage"
								 :instance name
								 :jvm (vmname)
								 :host (remote-hostname)}
								(double (.getUsed (MemoryUsage/from (.getValue the-attribute))))])))) 
				    
				    result))
				[] attributes)]
		(assoc r object-name fns))
	      r)) 
	  {} (object-names server)))

(defn thread-bean [server]
  (let [bean (. JMX newMXBeanProxy server
		      (ObjectName. "java.lang:type=Threading") ThreadMXBean)]
    {:bean bean :cpuTime (.isThreadCpuTimeSupported bean) :contention (.isThreadContentionMonitoringSupported bean)}
    ))

(defn thread-info [thread-bean]
  (let [bean (:bean thread-bean)
	threadinfo (.getThreadInfo bean (.getAllThreadIds bean))
	result (atom {})]
    (dorun (map (fn [thread-info]
		  (when (and (:cpuTime thread-bean) (.isThreadCpuTimeEnabled bean))
		    (swap! result (fn [current] (assoc current {:category "Thread" 
								:counter "CPU Time"
								:instance (str (.getThreadName thread-info) ":" (.getThreadId thread-info)) 
								:jvm (vmname)
								:host (remote-hostname)}
						       (/ (.getThreadCpuTime bean (.getThreadId thread-info)) 1000000000.0)))))
		  (swap! result (fn [current] (assoc current {:category "Thread" 
								:counter "Block Count"
								:instance (str (.getThreadName thread-info) ":" (.getThreadId thread-info)) 
								:jvm (vmname)
								:host (remote-hostname)}
						       (double (.getBlockedCount thread-info)))))
		  (swap! result (fn [current] (assoc current {:category "Thread" 
								:counter "Waited Count"
								:instance (str (.getThreadName thread-info) ":" (.getThreadId thread-info)) 
								:jvm (vmname)
								:host (remote-hostname)}
						       (double (.getWaitedCount thread-info)))))
		  (when (and (:contention thread-bean) (.isThreadContentionMonitoringEnabled bean))
		    (swap! result (fn [current] (assoc current {:category "Thread" 
								:counter "Waited Time"
								:instance (str (.getThreadName thread-info) ":" (.getThreadId thread-info)) 
								:jvm (vmname)
								:host (remote-hostname)}
						       (/ (.getWaitedTime thread-info) 1000.0))))
		     (swap! result (fn [current] (assoc current {:category "Thread" 
								:counter "Blocked Time"
								:instance (str (.getThreadName thread-info) ":" (.getThreadId thread-info)) 
								:jvm (vmname)
								:host (remote-hostname)}
						       (/ (.getBlockedTime thread-info) 1000.0)))))
						       

	      ) threadinfo))
    @result))

(defn memory-values [mem-fns server]
  (reduce (fn [result a-fn] 
	    (let [attributeList (.asList (.getAttributes server (key a-fn) (into-array ["Name" 
											"Usage" ])))]
	      (reduce (fn [re aval]
			(let [r (aval attributeList)]
			(apply assoc re r))) 
		      result (val a-fn)))) 
	  {} mem-fns))

(defn- jmx-java6-impl
  ([stop-fn threads?]
  (let [collector-fns (collector-keys (mbean-server))
	mem-fns (memory-fns (mbean-server))
	thread-beans (thread-bean (mbean-server))]
    (while (not (stop-fn))
	   (let [the-time (remote-time)]

	     (dorun (map (fn [i]  
			   (add-data (key i) the-time (val i))) 
			 (collector-values collector-fns (mbean-server))))
	     (dorun (map (fn [i]  
			   (add-data (key i) the-time (val i))) 
			 (memory-values mem-fns (mbean-server))))
	     
	       (dorun (map (fn [i]  
			     (add-data (key i) the-time (val i) threads?)) 
			   (thread-info thread-beans)))

	     (add-data 
	      {:host (remote-hostname) 
	       :jvm (vmname) 
	       :category "Runtime" 
	       :counter "UpTime"}
		  the-time (double (remote-uptime TimeUnit/SECONDS))))))))

(defn jmx-java6 
 ([name port stop-fn]
     (using-named-jmx-port port name (jmx-java6-impl stop-fn false)))
 ([name host port stop-fn]
   (using-named-jmx host port name (jmx-java6-impl stop-fn false)))
 ([stop-fn] 
    (jmx-java6-impl stop-fn false)))
 
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
      (is (<= (first uptime-values) (second uptime-values)))))))


  
  


