(ns monitor.jmxcollection
					;  (:use (monitor database jmx) )
   (:use (monitor jmx) )
  (:use (clojure.contrib repl-utils))
  (:import (java.util.concurrent TimeUnit)
	   (javax.management JMX ObjectName MBeanServer MBeanServerConnection Attribute)  
	   (java.lang.management MemoryUsage ThreadMXBean ThreadInfo))
	   
					; for testing purposes
  (:use clojure.test)
					;for repl purposes
  (:use (clojure stacktrace inspector)))

(def *last-last-gc-info* (atom (sorted-map)))
(def *add* (fn [& _]))

(defn- object-names [#^MBeanServerConnection server]
  (. server queryNames nil nil))

(defn collector-keys [#^MBeanServerConnection server]
  (let [hasGcInfo  (try
		    (let [gcinfo-class (Class/forName "com.sun.management.GcInfo")
			  from-method (. gcinfo-class getMethod "from" (into-array [javax.management.openmbean.CompositeData]))
			  create (fn [#^Attribute at]; (println "create called") 
				   (.invoke from-method nil (to-array [(.getValue at)])))]
		      create)
		    (catch ClassNotFoundException _ nil))]
							    
  (reduce (fn [result #^ObjectName object-name] 
	    (if (re-matches #"java.lang:type=GarbageCollector,name=.*" 
				       (. object-name toString))
	      (let [attributes (seq (. server getAttributes 
				   object-name (into-array ["Name" 
							    "CollectionCount" 
							    "CollectionTime" 
							    "LastGcInfo"])))
		    fns (reduce (fn [result #^Attribute attribut]
				  (condp = (.getName attribut)
				    "CollectionCount" (conj result (fn [attributes] 
								     (when-let [the-attribute (some (fn [#^Attribute a] (when (= "CollectionCount" (.getName a))
												       a)) attributes)]
								       (when-let [name (some (fn [#^Attribute a] (when (= "Name" (.getName a)) (.getValue a))) attributes)] 

									 [{:category "GarbageCollector"
									   :counter "Count"
									   :instance name
									   :jvm (vmname)
									   :host (remote-hostname)}
									  (.getValue #^Attribute the-attribute)]))))

				    "CollectionTime" (conj result (fn [attributes] 
								     (when-let [the-attribute (some (fn [#^Attribute a](when (= "CollectionTime" (.getName a)) 
												       a)) attributes)]
								       (when-let [name (some (fn [#^Attribute a] (when (= "Name" (.getName a)) (.getValue a))) attributes)] 
								       [{:category "GarbageCollector"
									:counter "Time"
									:instance name
									:jvm (vmname)
									:host (remote-hostname)}
									(/ (.getValue #^Attribute the-attribute) 1000.0)]))))

				    "LastGcInfo"  (if hasGcInfo 
						    (conj result (fn [attributes] 
								   (when-let [the-attribute (some (fn [#^Attribute a] (when (= "LastGcInfo" (.getName a)) 
												     a)) attributes)]
								     (when-let [name (some (fn [#^Attribute a](when (= "Name" (.getName a)) (.getValue a))) attributes)] 
								       (when-let [#^com.sun.management.GcInfo gcinfo (hasGcInfo the-attribute)]
									 (let [#^java.util.Map before-gc (.getMemoryUsageBeforeGc gcinfo)
									       #^java.util.Map after-gc (.getMemoryUsageAfterGc gcinfo)
									       timestamp (.getEndTime gcinfo)
									       result [{:category "GarbageCollector"
											:counter "Last Duration"
											:section name
											:jvm (vmname)
											:host (remote-hostname)}
										       (/ (.getDuration gcinfo) 1000.0)]
									       usage-before (reduce (fn [result #^java.util.Map area]
					;		  (println (str "Before " (key area) " " name " " (.getUsed (val area))))
												      (conj result {:category "GarbageCollector"
														    :counter "Usage Before Last"
														    :instance (key area)
														    :jvm (vmname)
														    :host (remote-hostname)
														    :section name}
													    (.getUsed #^java.lang.management.MemoryUsage (val area)))) [] before-gc)
									       committed-after (reduce (fn [result area]
													 (conj result {:category "GarbageCollector"
														       :counter "Committed After Last"
														       :instance (key area)
														       :jvm (vmname)
														       :host (remote-hostname)
														       :section name}
													        (.getCommitted #^java.lang.management.MemoryUsage (val area)))) [] after-gc)
									       max-after (reduce (fn [result area]
												   (conj result {:category "GarbageCollector"
														 :counter "Max After Last"
														 :instance (key area)
														 :jvm (vmname)
														 :host (remote-hostname)
														 :section name}
													 (.getMax #^java.lang.management.MemoryUsage (val area)))) [] after-gc)
									       
									       usage-after (reduce (fn [result area]
												     (conj result {:category "GarbageCollector"
														   :counter "Usage After Last"
														   :instance (key area)
														   :jvm (vmname)
														   :host (remote-hostname)
														   :section name}
													   (.getUsed #^java.lang.management.MemoryUsage (val area)))) [] after-gc)]
									   (swap! *last-last-gc-info* assoc timestamp [usage-before usage-after committed-after max-after])
									   (concat result usage-before usage-after committed-after max-after))
									 )))))
						    result)
				    result)) [] attributes)]
		(assoc result object-name fns))
	      result))
	  {} (object-names server))))

(defn collector-values [collector-keys #^MBeanServerConnection server]
  (binding [*last-last-gc-info* (atom (sorted-map))]
  (let [result (reduce (fn [result a-key] 
	    (let [attributeList (.asList (.getAttributes server (key a-key) (into-array ["Name" 
							    "CollectionCount" 
							    "CollectionTime" 
							    "LastGcInfo"])))]
	      (reduce (fn [re aval]
			(let [r (aval attributeList)]

			  (if r
			    (apply assoc re r)
			    re))) 
		      result (val a-key)))) 
		       {} collector-keys)]

    (if (seq @*last-last-gc-info*)
      (reduce (fn [r e]
		(reduce (fn [r2 f]
			  (let [to (assoc (first f) :section "Latest")
				v (second f)]
			    (assoc r2 to v)))
			r
			(partition 2 e)))
	      result
	      (val (last @*last-last-gc-info*)))
      result))))


(defn memory-fns [#^MBeanServerConnection server]
  (reduce (fn [r #^ObjectName object-name] 
	    (if (re-matches #"java.lang:type=MemoryPool,name=.*" (. object-name toString))
	      (let [attributes (seq (. server getAttributes 
				       object-name (into-array ["Usage" 
								"Name"])))
		    fns (reduce (fn [result #^Attribute attribut]
				  (condp = (.getName attribut)
				    "Usage" (conj result (fn [attributes] 
							   (when-let [#^Attribute the-attribute (some (fn [#^Attribute a] (when (= "Usage" (.getName a))
											     a)) attributes)]
							     (when-let [name (some (fn [#^Attribute a] (when (= "Name" (.getName a)) (.getValue a))) attributes)]
							       ;(println (str "Use " name " " (.getUsed (MemoryUsage/from (.getValue the-attribute)))))
							       [{:category "Memory"
								 :counter "Usage"
								 :instance name
								 :jvm (vmname)
								 :host (remote-hostname)}
								(.getUsed (MemoryUsage/from (.getValue the-attribute)))])))) 
				    
				    result))
				[] attributes)]
		(assoc r object-name fns))
	      r)) 
	  {} (object-names server)))

(defn thread-bean [#^MBeanServerConnection server]
  (let [#^ThreadMXBean bean (. JMX newMXBeanProxy server
		      (ObjectName. "java.lang:type=Threading") ThreadMXBean)]
    {:bean bean :cpuTime (.isThreadCpuTimeSupported bean) :contention (.isThreadContentionMonitoringSupported bean)}
    ))

(defn thread-info [thread-bean]
  (let [bean #^ThreadMXBean (:bean thread-bean)
	threadinfo #^ThreadInfo (.getThreadInfo bean (.getAllThreadIds bean))
	result (atom {})]
    (dorun (map (fn [#^ThreadInfo thread-info]
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

(defn memory-values [mem-fns #^MBeanServerConnection server]
  (reduce (fn [result a-fn] 
	    (let [attributeList (.asList (.getAttributes server (key a-fn) (into-array ["Name" 
											"Usage" ])))]
	      (reduce (fn [re aval]
			(let [r (aval attributeList)]
			(apply assoc re r))) 
		      result (val a-fn)))) 
	  {} mem-fns))

(defn- add-memory-usage
  [pid the-time]
  (let [memoryusage (remote-memory)
	heap ^java.lang.management.MemoryUsage (:heap memoryusage)
	non-heap ^java.lang.management.MemoryUsage (:non-heap memoryusage)]
    (*add* {:host (remote-hostname)
	    :jvm (vmname)
	    :pid pid
	    :category "Memory"
	    :counter "Heap used"}
	   the-time (.getUsed heap))
    (*add* {:host (remote-hostname)
	    :jvm (vmname)
	    :pid pid
	    :category "Memory"
	    :counter "Heap committed"}
	   the-time (.getCommitted heap))
    (*add* {:host (remote-hostname)
	    :jvm (vmname)
	    :pid pid
	    :category "Memory"
	    :counter "% Heap committed"}
	   the-time (* 100.0 (/ (.getCommitted heap) (.getMax heap))))
    (*add* {:host (remote-hostname)
	    :jvm (vmname)
	    :pid pid
	    :category "Memory"
	    :counter "% Heap used"}
	   the-time  (* 100.0 (/ (.getUsed heap) (.getMax heap))))
    (*add* {:host (remote-hostname)
	    :jvm (vmname)
	    :pid pid
	    :category "Memory"
	    :counter "Non-Heap committed"}
	   the-time (.getCommitted non-heap))
    (*add* {:host (remote-hostname)
	    :jvm (vmname)
	    :pid pid
	    :category "Memory"
	    :counter "Non-Heap used"}
	   the-time (.getUsed non-heap))))

(defn- jmx-java6-impl
  ([stop-fn threads?]
  (let [collector-fns (collector-keys (mbean-server))
	mem-fns (memory-fns (mbean-server))
	thread-beans (thread-bean (mbean-server))
	pid (remote-pid)
	percent-of-heap (fn percent-of-heap [col-vals the-time]
			  (doseq [per-heap (partition-by #(:instance (key %)) (sort-by #(:instance (key %)) col-vals))]
			    (let [commited-and-max (sort-by #(:counter (key %))
							    (filter #(and (= "Latest" (:section (key %)))
									  (or (= "Max After Last" (:counter (key %)))
									      (= "Committed After Last" (:counter (key %)))
									      (= "Usage After Last" (:counter (key %)))))
								    per-heap))]
			      (when (= 3 (count commited-and-max))
				(when-not  (some nil? (vals commited-and-max))
				  (let [committed (val (first commited-and-max))
					max (val (second commited-and-max))
					usage (val (nth commited-and-max 2))]
				    (when-not (or (zero? max) (zero? usage))
				      (*add* (assoc (key (first commited-and-max)) :pid pid :counter "% Committed after Last")
					     the-time
					     (* 100.0 (/ committed max)))
				      (*add* (assoc (key (first commited-and-max)) :pid pid :counter "% Usage after Last")
					     the-time
					     
					     (* 100.0 (/ usage committed))))))))))]
    (while (not (stop-fn))
	   (let [the-time (remote-time)]
	     (let [cv (collector-values collector-fns (mbean-server))]
	       (dorun (map (fn [i]  
			     (*add* (assoc (key i) :pid pid) the-time (val i))) 
			   cv))
	       (percent-of-heap cv the-time))
	     (dorun (map (fn [i]  
			   (*add* (assoc (key i) :pid pid) the-time (val i))) 
			 (memory-values mem-fns (mbean-server))))

	       #_(dorun (map (fn [i]  
			     (*add* (assoc (key i) :pid pid) the-time (val i) threads?)) 
			   (thread-info thread-beans)))

	       (*add* {:host (remote-hostname)
			    :jvm (vmname)
			    :pid pid
			   :category "Thread"
			   :counter "Number fo Threads"}
			 the-time (remote-threads))
	       
	       (add-memory-usage pid the-time)
	       
	     (*add* 
	      {:host (remote-hostname) 
	       :jvm (vmname)
	       :pid pid
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


#_(deftest test-java6
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


  
  


