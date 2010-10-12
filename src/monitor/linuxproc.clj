(ns monitor.linuxproc
  (:import (java.io BufferedReader FileReader InputStreamReader OutputStreamWriter PushbackReader))
  (:import (java.util Date))
  (:import (java.util.concurrent Executors))
  (:import (java.util.concurrent.locks ReentrantLock))
  (:import (java.net SocketTimeoutException ServerSocket))
  (:use (clojure pprint test))
  (:use (monitor database))
  (:use [clojure.contrib.logging :only [info]]))

(defn net-dev-fn []
  (let [last-time (atom nil)
	last-values (atom nil)
	max-value 4294967295
	calculate (fn calculate [last current seconds]
		    (if (< current last)
		      (/ (- (+ current max-value) last) seconds)
		    (/ (- current last) seconds)))]
    (fn net-dev []
      (let [now (System/currentTimeMillis)
	    stream (BufferedReader. (FileReader. "/proc/net/dev"))]
	(try
	  (.readLine stream)
	  
	  (let [header (.readLine stream)
		header-pattern #"[^|]+\|bytes\s+packets\s+errs\s+drop\s+fifo\s+frame\s+compressed\s+multicast\|bytes\s+packets\s+errs\s+drop\s+fifo\s+colls\s+carrier\s+compressed\s*"
		value-pattern #"^\s*([^:]+):\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$"
		lines (line-seq stream)]
	    (if (re-matches header-pattern header)
	      (if-let [match (re-matches header-pattern header)]
		(let [values (reduce (fn [r line]
				       (if-let [match (re-matches value-pattern line)]
					 (assoc r (second match)
						{:received-bytes (Long/parseLong (nth match 2))
						 :received-packets (Long/parseLong (nth match 3))
						 :receive-errors (Long/parseLong (nth match 4))
						 :receive-drops (Long/parseLong (nth match 5))
						 :sent-bytes (Long/parseLong (nth match 10))
						 :sent-packets (Long/parseLong (nth match 11))
						 :send-errors (Long/parseLong (nth match 12))
						 :send-drops (Long/parseLong (nth match 13))
						 :send-collisions (Long/parseLong (nth match 14))
						 })
					 r))
				     {} lines)]
		   (try
		  (if @last-values
		    (let [seconds (/ (-  now @last-time) 1000.0)]
		     
			(reduce (fn [r e]
				  (let [lv (get @last-values (key e))
					cv (val e)]
				    (assoc r (key e)
					   {"Received bytes" (calculate (:received-bytes lv) (:received-bytes cv) seconds)
					    "Received packets" (calculate (:received-packets lv) (:received-packets cv) seconds)
					    "Receive errors" (calculate (:receive-errors lv) (:receive-errors cv) seconds)
					    "Receive drops" (calculate (:receive-drops lv) (:receive-drops cv) seconds)
					    "Sent bytes" (calculate (:sent-bytes lv) (:sent-bytes cv) seconds)
					    "Sent packets" (calculate (:sent-packets lv) (:sent-packets cv) seconds)
					    "Send errors" (calculate (:send-errors lv) (:send-errors cv) seconds)
					    "Send drops" (calculate (:send-drops lv) (:send-drops cv) seconds)
					    "Send collisions" (calculate (:send-collisions lv) (:send-collisions cv) seconds)}
					   ))   
				  ) {} values)
			
		  
	      ))(finally
			 (reset! last-values values)
			 (reset! last-time now)
			 ))))))
      (finally (.close stream)))))))

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

(defn sys-load []
  (let [pattern #"^\d+\.\d+\s\d+\.\d+\s(\d+\.\d+)\s(\d+)/(\d+)\s.*"
	stream (BufferedReader. (FileReader. "/proc/loadavg"))]
    (try
      (if-let [vals (next (re-matches pattern (.readLine stream)))]
	{"average active threads/min" (Double/parseDouble (first vals))
	 "active threads" (Long/parseLong (second vals))
	 "threads" (Long/parseLong (second (next vals)))})
      (finally
       (.close stream))))
  )

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
	      (reduce (fn [r e]
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
							   (assoc r "context switches/s" (/ (- (val e) l) seconds)))
							 r)
			 (= :interrupts (key e)) (if-let [l (:interrupts @last-cpu)]
							 (let [seconds (/ (- timestamp @last-time) 1000.0)]
							   (assoc r "interrupts/s" (/ (- (val e) l) seconds)))
							 r)
			 :else r)
	      ) {} new-values)
	      (finally
	       (reset! last-time timestamp)
	       (reset! last-cpu new-values))))
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
			  "buffers" (:buffers values)))]
	    
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



(defn to-text [net-dev disk cpu]
(pr-str (assoc {} (.getHostName (java.net.InetAddress/getLocalHost)) (assoc {} "Network Device" (net-dev) "Disk" (disk) "Cpu" (cpu) "Memory" (mem) "Load" (sys-load)))))



(defn paths [map-tree]
  (loop [result {}
	 path []
	 stack []
	 in (seq map-tree)]
    (if-let [cur (first in)]
      (let [following (next in)
	    is-map (map? (val cur))]
	(if is-map
	  (recur result,
		 (conj path (key cur)),
		 (conj stack following),
		 (val cur))
	  (recur (assoc result (conj path (key cur)) (val cur)),
		 path,
		 stack,
		 following)))
      (if-not (empty? stack)
	(recur result
	       (pop path)
	       (pop stack)
	       (last stack))
	result))))

(defn from-text [text]
  (paths (read (PushbackReader. (java.io.StringReader. text)))))

(defn process-remote-linux-proc
  ([socket stop]
     (try
       (let [input (PushbackReader. (InputStreamReader. (.getInputStream socket)))
	     data (repeatedly #(if (stop)
				 (throw (proxy [java.lang.Exception] [] (toString ([] "Stopped"))))
				 (paths (read input))))]
	 (dorun (map (fn [d]
		       (let [timestamp (Date.)]
			 (dorun (map (fn [e]
				       (let [name (let [names (key e)]
						    (if (= 4 (count names))
						      (zipmap [:host :category :instance :counter] names)
						      (zipmap [:host :category :counter] names)))]
					 (add-data name timestamp (val e))))
				     d))))
		     data)))
       (catch Exception e
	 (if (.contains (.getMessage e) "EOF" )
	   (info (str "Lost contact with linux proc process at " (.getHostName (.getRemoteSocketAddress socket))))
	   
	   (throw e)))))
  ([host port stop]
     (try 
       (process-remote-linux-proc (java.net.Socket. host port) stop)
       (catch java.net.ConnectException e
	 (info (str "Nobody is listening on " host "port" port))))))
  
(deftest test-path
  (is (= {["f"] 4, ["a" "e"] 3, ["a" "b" "d"] 2, ["a" "b" "c"] 1}
	 (paths {"a" {"b" {"c" 1 "d" 2} "e" 3} "f" 4})))
  (is (= {["a"] 1}
	 (paths {"a" 1})))
  (is (= {["h"] 5, ["a" "b"] 1, ["a" "c" "d"] 2, ["a" "c" "e" ] 3, ["a" "f" "g"] 4}
	 (paths {"a" {"b" 1 "c" {"d" 2 "e" 3} "f" {"g" 4}} "h" 5})))
  (is (= {["a" "b"] 1, ["a" "c" "d"] 2, ["a" "c" "e" ] 3, ["a" "f" "g"] 4 ["h" "i" "j" "k" "l"] 5 ["h" "i" "m"] 6}
	 (paths {"a" {"b" 1 "c" {"d" 2 "e" 3} "f" {"g" 4}} "h" {"i" {"j" {"k" {"l" 5}} "m" 6}}})))
  )

	    


(defn serve-linux-proc [port frequency]
  (let [server (ServerSocket. port)
	net-dev (net-dev-fn)
	disk (disk-fn)
	cpu (cpu-fn)
	sockets (atom #{})
	accept-executor (Executors/newSingleThreadExecutor
			   (proxy [java.util.concurrent.ThreadFactory] []
			     (newThread [runnable] (doto (Thread. runnable)
						     (.setDaemon true)))))
	condition (.newCondition (doto (ReentrantLock.)
				   (.lock)))
	]
    (. accept-executor execute (fn acceptor []
				 (.setSoTimeout server 1000)
				 (while true
				   (try
				     (if-let [socket (.accept server)]
				       (do (.setSoTimeout socket 2000)
					   (swap! sockets (fn [sockets] (conj sockets socket)))))
				     (catch SocketTimeoutException _)))
				 ))
    (.shutdown accept-executor)

    (loop [time (Date.)]
      (let [text (to-text net-dev disk cpu)]
	(dorun (map (fn [s]
		      (try
			(binding [*out* (OutputStreamWriter. (.getOutputStream s))]
			  (println text)
			  (flush))
			(catch Exception e
			  (println (.getMessage e))
			  (swap! sockets (fn [sockets] (disj sockets s ))))))
		    @sockets)))

      (.awaitUntil condition time)
      (when-not (.isTerminated accept-executor)
	(recur (Date. (+ (.getTime time) (* 1000 frequency))))))))
    
			    
