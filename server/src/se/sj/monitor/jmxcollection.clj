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

(defn collector-keys [server]
  (let [hasGcInfo  (try
		    (let [gcinfo-class (Class/forName "com.sun.management.GcInfo")
			  from-method (. gcinfo-class getMethod "from" (into-array [javax.management.openmbean.CompositeData]))
			  create #(.invoke from-method nil (to-array [(.getValue %)]))]
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
									  (.getValue the-attribute)]))))

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
						    (conj result (fn [attirbutes] 
								   (when-let [the-attribute (some #(when (= "LastGcInfo" (.getName %)) 
												     %) attributes)]
								     (when-let [name (some #(when (= "Name" (.getName %)) (.getValue %)) attributes)] 
								     (let [gcinfo (hasGcInfo the-attribute)
									   before-gc (.getMemoryUsageBeforeGc gcinfo)
									   after-gc (.getMemoryUsageAfterGc gcinfo)
									   result [{:category "GarbageCollector"
										    :counter "Last Duration"
										    :instance name
										    :jvm (vmname)
										    :host (remote-hostname)}
										   (/ (.getDuration gcinfo) 1000.0)]
									   usage-before (reduce (fn [result area]
												  (conj result {:category "GarbageCollector"
														 :counter "Usage Before Last"
														 :instance name
														 :jvm (vmname)
														 :host (remote-hostname)
														 :section (key area)}
													 (.getUsed (val area)))) [] before-gc)
									   committed-after (reduce (fn [result area]
												      (conj result {:category "GarbageCollector"
														    :counter "Committed After Last"
														    :instance name
														    :jvm (vmname)
														    :host (remote-hostname)
														    :section (key area)}
													    (.getCommitted (val area)))) [] after-gc)
									   max-after (reduce (fn [result area]
												      (conj result {:category "GarbageCollector"
														    :counter "Max After Last"
														    :instance name
														    :jvm (vmname)
														    :host (remote-hostname)
														    :section (key area)}
													    (.getMax (val area)))) [] after-gc)
									   usage-after (reduce (fn [result area]
												 (conj result {:category "GarbageCollector"
														:counter "Usage After Last"
														:instance name
														:jvm (vmname)
														:host (remote-hostname)
														:section (key area)}
													(.getUsed (val area)))) [] after-gc)]
								       (concat result usage-before usage-after committed-after max-after)
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

;(defn memory-fns [server]
;  (let [poolbean (. JMX newMXBeanProxy server
;		      (ObjectName. "java.lang:type=Runtime") RuntimeMXBean)
;)

(defn- jmx-java6-impl
  [stop-fn]
  (let [attribute-fns (collector-keys (mbean-server))]
    (while (not (stop-fn))
	   (let [the-time (remote-time)]

	     (dorun (map (fn [i]  
			   (add-data (key i) the-time (val i))) 
			 (collector-values attribute-fns (mbean-server))))
	     (add-data 
	      {:host (remote-hostname) 
	       :jvm (vmname) 
	       :category "Runtime" 
	       :counter "UpTime"}
		  the-time (remote-uptime TimeUnit/SECONDS))))))

(defn jmx-java6 
 ([port stop-fn]
     (using-jmx-port port (jmx-java6-impl stop-fn)))
 ([host port stop-fn]
   (using-jmx host port (jmx-java6-impl stop-fn)))
 ([stop-fn] 
    (jmx-java6-impl stop-fn)))
 
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


  
  


