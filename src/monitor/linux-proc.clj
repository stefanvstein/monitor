(ns monitor.linux-proc
  (:import (java.io BufferedReader FileReader))
  (:use (clojure pprint))
  )

(defn net-dev []
  (let [stream (BufferedReader. (FileReader. "/proc/net/dev"))]
    (try
      (.readLine stream)
      
      (let [header (.readLine stream)
	    header-pattern #"[^|]+\|bytes\s+packets\s+errs\s+drop\s+fifo\s+frame\s+compressed\s+multicast\|bytes\s+packets\s+errs\s+drop\s+fifo\s+colls\s+carrier\s+compressed\s*"
	    value-pattern #"^\s*([^:]+):\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$"
	    lines (line-seq stream)]
	(if (re-matches header-pattern header)
	  (if-let [match (re-matches header-pattern header)]
	    (reduce (fn [r line]
		      (if-let [match (re-matches value-pattern line)]
			(assoc r (second match)
			       {"Received bytes" (nth match 2)
				"Received packets" (nth match 3)
				"Receive errors" (nth match 4)
				"Receive drops" (nth match 5)
				"Sent bytes" (nth match 10)
				"Sent packets" (nth match 11)
				"Send errors" (nth match 12)
				"Send drops" (nth match 13)
				"Send collisions" (nth match 14)})
			r))
		    {} lines))))
      (finally (.close stream)))))

(defn disk-fn []
  (let [pattern #"[\s0-9]*(sd[a-z]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).*"
	max-value 4294967295
	last-disk (atom nil)
	last-time (atom nil)]
    (fn disk []
      (let [stream (BufferedReader. (FileReader. "/proc/diskstats"))]
	(try
	  
	  (let [now (System/currentTimeMillis)
		new-values
		(reduce (fn [r line]
			  (if-let [m (re-matches pattern line)]
			    
			    (assoc r (nth m 1) (assoc {}  :written (/ (Long/parseLong (nth m 8)) 2)
						      :read (/ (Long/parseLong (nth m 4)) 2)
						      :queue (Long/parseLong (nth m 10))))
			    r)) {} (line-seq stream))]
	   
	    (try
	      (if-let [ldisk @last-disk]
		(let [seconds (/ (- now @last-time) 1000.0)
		      change (fn change [new-value old-value]
			       (if (< new-value old-value)
				 (- (+ new-value max-value) old-value)
				 (- new-value old-value)))]
		  (reduce (fn [r e]
			    (assoc r (key e) (assoc {}
					       "written kB/s" (/ (change (:written (val e)) (:written (get ldisk (key e)))) seconds)
					       "read kB/s" (/ (change (:read (val e)) (:read (get ldisk (key e)))) seconds)
					       "queue" (:queue (val e)))))
			  {}
			  new-values))
		(reduce (fn [r e]
			  (assoc r (key e) {"queue" (:queue (val e))}))
			{}
			new-values))
	      (finally
	       (reset! last-disk new-values)
	       (reset! last-time now))))
	    (finally
	     (.close stream)))))))

(defn sys-load [])

(defn cpu-fn []
  (let [value-pattern #"^cpu(\d*)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).*"
	last-cpu (atom {})
	last-time (atom nil)]
    (fn cpu []
      (let [stream (BufferedReader. (FileReader. "/proc/stat"))
	    timestamp (System/currentTimeMillis)]
	(try
	  (let [new-values (reduce (fn [r line]
		    (cond
		     (.startsWith line "ctxt") (assoc r :context-switches (Long/parseLong (.trim (.substring line 5))))
					
		     (.startsWith line "intr") (assoc r :interrupts (Long/parseLong (nth (re-matches #"intr\s+(\d+)\s+.*" line) 1)))
					
		     (.startsWith line "cpu") (let [m (re-matches value-pattern line)
						    cpu (let [cpu (first (rest m))]
							  (if (= "" cpu)
							    "Total"
							    cpu))
						    
						    user (Long/parseLong (nth m 2))
						    nice (Long/parseLong (nth m 3))
						    system (Long/parseLong (nth m 4))
						    idle (Long/parseLong (nth m 5))
						    iowait (Long/parseLong (nth  m 6))
						    irq (Long/parseLong (nth m 7))
						    softirq (Long/parseLong (nth m 8))]
						(assoc r cpu {:user user
							      :nice nice
							      :system system
							      :idle idle
							      :iowait iowait
							      :irq irq
							      :softirq softirq}))))
		  {} (filter #(or
			       (.startsWith % "cpu")
			       (.startsWith % "ctxt")
			       (.startsWith % "intr"))
			     (line-seq stream)))]
	    (try
;	      (println "new-values")
;		       (pprint new-values)
;		       (println "last-cpu")
;		       (pprint @last-cpu)
	      (reduce (fn [r e]
			;(println (key e) (= String (class (key e))))
			(cond
			 (= String (class (key e)))
			 (if-let [l (get @last-cpu (key e))]
			   (if-let [cur (val e)]
			   (let [user (- (:user cur) (:user l))
				 nice (- (:nice cur) (:nice l))
				 system (- (:system cur) (:system l))
				 idle (- (:idle cur) (:idle l))
				 iowait (- (:iowait cur) (:iowait l))
				 irq (- (:irq cur) (:irq l))
				 softirq (- (:softirq cur) (:softirq l))
				 sum (+ user nice system idle iowait irq softirq)
				 p (fn [a ] (if (zero? sum)
					      0
					      (double (* 100 (/ a sum)))))]
			     
			     (assoc r (key e) {"user" (p user)
					   "nice" (p nice)
					   "system" (p system)
					   "idle" (p idle)
					   "iowait" (p iowait)
					   "irq" (p irq)
					   "softirq" (p softirq)})
			     )r ) r)
			 (= :context-switches (key e)) (if-let [l (:context-switches @last-cpu)]
							 (let [seconds (/ (- timestamp @last-time) 1000.0)]
							   (assoc r "context switches/s" (/ (- (val e) l) seconds))))
							 
			 (= :interrupts (key e)) (if-let [l (:interrupts @last-cpu)]
							 (let [seconds (/ (- timestamp @last-time) 1000.0)]
							   (assoc r "interrupts/s" (/ (- (val e) l) seconds))))
			 :else r)
	      ) {} new-values)
	      
	      (finally
	       (reset! last-time timestamp)
	       (reset! last-cpu new-values)
	       ))
	  
					;out with calculations
	    )
	  (finally (.close stream)))))))
  
  (defn mem []
    (let [memtotal-p #"^MemTotal:\s+(\d+)\s+kB.*" ;physical total
	  memfree-p #"^MemFree:\s+(\d+)\s+kB.*";LowFree+HighFree, that is physical free
	  buffers-p #"^Buffers:\s+(\d+)\s+kB.*";Mem in buffer cache
	  cached-p #"^Cached:\s+(\d+)\s+kB.*";Disk cache - swap cache
	  swap-cache-p #"^SwapCached:\s+(\d+)\s+kB.*"; Swaped mem in disk cache
	  active-p #"^Active:\s+(\d+)\s+kB.*";Memory that has been used more recently and usually not reclaimed unless absolutely necessary.
	  dirty-p #"^Dirty:\s+(\d+)\s+kB.*";Might need writing to disk
	  low-total-p #"^LowTotal:\s+(\d+)\s+kB.*";Total avail for kernel
	  low-free-p #"^LowFree:\s+(\d+)\s+kB.*";Free for kernel, might be occupied by others too
	  swap-total-p #"^SwapTotal:\s+(\d+)\s+kB";swap memory 
	  swap-free-p #"^SwapFree:\s+(\d+)\s+kB";swap free
	  slab-p #"^Slab:\s+(/d+)\s+kB" ;Kernel data structure cache
	  mapped-p #"^Mapped:\s+(\d+)\s+kB.*"
	  writeback-p #"^Writeback:\s+(\d+)\s+kB.*"
	  inactive-p #"^Inactive:\s+(\d+)\s+kB.*"
	  patterns (assoc (sorted-map) :total-mem memtotal-p
			  :free-mem memfree-p
			  :buffers buffers-p
			  :cached cached-p
			  :swap-cache swap-cache-p
			  :active active-p
			  :dirty dirty-p
			  :total-low low-total-p
			  :free-low low-free-p
			  :swap-total swap-total-p
			  :swap-free swap-free-p
			  :mapped mapped-p
			  :slab slab-p
			  :inactive inactive-p
			  :writeback writeback-p)
	  patterns-left (atom patterns)
	  stream (BufferedReader. (FileReader. "/proc/meminfo"))]
      (try
	(let [values (reduce (fn [r line]
			       (loop [patterns @patterns-left]
				 (if-let [m (re-matches (val (first patterns)) line)]
				   (do (swap! patterns-left #(dissoc % (key (first patterns))))
				       (assoc r (key (first patterns)) (Long/parseLong (second m))))
				   (if-let [remaining (next patterns)]
				     (recur remaining)
				     r))))
			     
			     {} (line-seq stream))]
	  (let [result (atom 
			(assoc {}
			  "physical free" (:free-mem values)
			  "physical used" (- (:total-mem values) (:free-mem values))
			  "swap free" (:swap-free values)
			  "swap in use" (- (:swap-total values) (:swap-free values) (:swap-cache values))
			  "swap cached"  (:swap-cache values)
			  "dirty" (:dirty values)
			  "active" (:active values)
			
			  "cache" (+ (:cached values) (:swap-cache values))
			  "buffers" (:buffers values)))
		
		]
	    (println (:swap-cache values))
	    (when (:free-low values)
	      (swap! result #(assoc %   "low free" (:free-low values)
				    "low used" (- (:total-low values) (:free-low values)))))
	    (when (:inactive values)
	      (swap! result #(assoc %   "inactive" (:inactive values))))
	    
	    (when (:writeback values)
	      (swap! result #(assoc %  "dirty" (+ (:dirty values) (:writeback values)))))
	    (when-let [slab (:slab values)]
	      (swap! result #(assoc % "slab" (:slab values))))
	    (when-let [slab (:mapped values)]
	      (swap! result #(assoc % "mapped" (:mapped values))))
	    @result))
	    
	      
	    
	      
      (finally
       (.close stream)))))
  