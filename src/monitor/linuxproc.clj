(ns monitor.linuxproc
  (:import (java.io File FileFilter IOException BufferedReader FileReader InputStreamReader OutputStreamWriter PushbackReader))
  (:import (java.util Date))
  (:import (java.util.concurrent Executors))
  (:import (java.util.concurrent.locks ReentrantLock))
  (:import (java.net SocketTimeoutException ServerSocket Socket SocketAddress))
  (:use (clojure pprint test stacktrace))
  (:use (monitor database termination shutdown))
  (:use [clojure.contrib.logging :only [info]]))

(defn interesting-pids
  "returns seq of directories in proc for those pids with command line matching re"
  [re]
  (let [pids-in-proc
	(seq (.listFiles (File. "/proc") (proxy [FileFilter] []
					   (accept [^File file]
						   (boolean (and
							     (.isDirectory file)
							     (re-matches #"^\d+$" (.getName file))))))))]
    (filter (fn [^File file]
	      (try
		(with-open [cmdline (BufferedReader. (FileReader. (File. file "cmdline")))]
		  (if-let [line (.readLine cmdline)]
		    (re-find re line)
		    nil))
		(catch IOException _
		  false)))
	    pids-in-proc)))

(def pids-of-interest (atom []))


(defn net-dev-fn []
  (let [last-time (atom nil)
	last-values (atom nil)
	max-value 4294967295
	calculate (fn calculate [last current seconds]
		    (if (< current last)
		      (/ (- (+ current max-value) last) seconds)
		    (/ (- current last) seconds)))]
    (fn net-dev []
      (let [now (System/currentTimeMillis)]
	(with-open [stream (BufferedReader. (FileReader. "/proc/net/dev"))]
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
						 :send-collisions (Long/parseLong (nth match 14))})
					 r))
				     {}
				     lines)]
		  (try
		    (if @last-values
		      (let [seconds (/ (-  now @last-time) 1000.0)]
			
			(reduce (fn [r e]
				  (let [lv (get @last-values (key e))
					cv (val e)]
				    (assoc r (key e)
					   {"Received kB/s" (/ (calculate (:received-bytes lv) (:received-bytes cv) seconds) 1024.0)
					    "Received packets/s" (calculate (:received-packets lv) (:received-packets cv) seconds)
					    
					    "Sent kB/s" (/ (calculate (:sent-bytes lv) (:sent-bytes cv) seconds) 1024.0)
					    "Sent packets/s" (calculate (:sent-packets lv) (:sent-packets cv) seconds)})))
				{}
				values)))
		  (finally
			 (reset! last-values values)
			 (reset! last-time now))))))))))))

(defn disk-fn []
  (let [pattern #"[\s0-9]*(sd[a-z]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).*"
	max-value 4294967295
	last-disk (atom nil)
	last-time (atom nil)]
    (fn disk []
      (with-open [stream (BufferedReader. (FileReader. "/proc/diskstats"))]
	  (let [now (System/currentTimeMillis)
		new-values
		(reduce (fn [r line]
			  (if-let [m (re-matches pattern line)]
			    
			    (assoc r (nth m 1) (assoc {}  :written (/ (Long/parseLong (nth m 8)) 2) ;7
						      :writes (Long/parseLong (nth m 6))
						      :read (/ (Long/parseLong (nth m 4)) 2) ;3
						      :reads (Long/parseLong (nth m 2))
						      :queue (Long/parseLong (nth m 10)))) ;9
			    r)) {} (line-seq stream))]
	   
	    (try
	      (if-let [ldisk @last-disk]
		(let [seconds (/ (- now @last-time) 1000.0)
		      change (fn change [new-value old-value]
			       (if (or (nil? new-value) (nil? old-value))
				 0
			       (if (< new-value old-value)
				 (- (+ new-value max-value) old-value)
				 (- new-value old-value))))]
		  (reduce (fn [r e]
			    (assoc r (key e) (assoc {}
					       "written kB/s" (/ (change (:written (val e)) (:written (get ldisk (key e)))) seconds)
					       "read kB/s" (/ (change (:read (val e)) (:read (get ldisk (key e)))) seconds)
					       "writes/s" (/ (change (:writes (val e)) (:writes (get ldisk (key e)))) seconds)
					       "reads/s" (/ (change (:reads (val e)) (:reads (get ldisk (key e)))) seconds)
					       "queue" (:queue (val e)))))
			  {}
			  new-values))
		(reduce (fn [r e]
			  (assoc r (key e) {"queue" (:queue (val e))}))
			{}
			new-values))
	      (catch Exception e
		(print-cause-trace e))
	      (finally
	       (reset! last-disk new-values)
	       (reset! last-time now))))))))

(defn sys-load []
  (let [pattern #"^\d+\.\d+\s\d+\.\d+\s(\d+\.\d+)\s(\d+)/(\d+)\s.*"]
	(with-open [stream (BufferedReader. (FileReader. "/proc/loadavg"))]
	  (when-let [vals (next (re-matches pattern (.readLine stream)))]
	    {"average active threads/min" (Double/parseDouble (first vals))
	     "active threads" (Long/parseLong (second vals))
	     "threads" (Long/parseLong (second (next vals)))}))))

(defn fd []
   (let [p #"(?m)Max open files\s+(\d+|unlimited)"
	 limits #(try
		   (when-let [r (re-find p (slurp (File. % "limits")))]
		     (let [max-files (second r)]
		       (when (not (= max-files "unlimited"))
			 (Integer/valueOf max-files))))
		  (catch Exception _))
	 fds #(try
	       (count (.list (File. % "fd")))
	       (catch Exception _))]
     {"process" (into {} (filter identity (for [pid @pids-of-interest]
				 (when-let [file-descs (fds pid)]
				   (let [pidval (Integer/valueOf (.getName pid))
					 res {"All" file-descs}]
				     [pidval (if-let [maximum (limits pid)]
					       (assoc res "% All" (* 100.0 (/ file-descs maximum)))
					       res)])))))}))
			    
	
	 
  
(defn cpu-fn []
  (let [value-pattern #"^cpu(\d*)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).*"
	last-cpu (atom {})
	last-time (atom nil)
	percent (fn [a sum] (if (zero? sum)
			      0
			      (* 100.0 (/ (double a) (double sum)))))
	cpu-jiffies (fn [total-new total-old]
		      (let [user (- (:user total-new) (:user total-old))
			    nice (- (:nice total-new) (:nice total-old))
			    system (- (:system total-new) (:system total-old))
			    idle (- (:idle total-new) (:idle total-old))
			    iowait (- (:iowait total-new) (:iowait total-old))
			    irq (- (:irq total-new) (:irq total-old))
			    softirq (- (:softirq total-new) (:softirq total-old))
			    sum (+ user nice system idle iowait irq softirq)]
			{:user user :nice nice :system system :idle idle :iowait iowait :irq irq :softirq softirq :sum sum}))]
    (fn cpu []
      (let [
	    timestamp (System/currentTimeMillis)
	    pids-dirs @pids-of-interest]
	(with-open [stream (BufferedReader. (FileReader. "/proc/stat"))]
	  (let [new-values (merge (reduce (fn [r ^String line]
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
					  {} (filter (fn [^String x] (or
						       (.startsWith x "cpu")
						       (.startsWith x "ctxt")
						       (.startsWith x "intr")))
						     (line-seq stream)))
				  (reduce (fn [r ^File pid-dir]
					    (with-open [stat (BufferedReader. (FileReader. (File. pid-dir "stat")))]
					      (if-let [line (.readLine stat)]
						(let [g (re-find #"(\d+)\s\(.*\)\s(.*)" line)]
						  (if (= 3 (count g))
						      (let [stats (vec (seq (.split ^String (nth g 2) "\\s")))]
							(assoc r (Integer/parseInt (second g)) {:user (Long/parseLong (get stats 11)) :system (Long/parseLong (get stats 12))}))
						      r))
						  r))) {} pids-dirs))]
	    (try
	      (let [system (reduce (fn [r e]
				     (cond
				      (= String (class (key e)))
				      (if-let [l (get @last-cpu (key e))]
					(if-let [cur (val e)]
					  (let [jiffies (cpu-jiffies cur l)
						{sum :sum} jiffies]
					    (assoc r (key e) {"user" (percent (:user jiffies) sum)
							      "nice" (percent (:nice jiffies) sum)
							      "system" (percent (:system jiffies) sum)
							      "idle" (percent (:idle jiffies) sum)
							      "iowait" (percent (:iowait jiffies) sum)
							      "irq" (percent (:irq jiffies) sum)
							      "softirq" (percent (:softirq jiffies) sum)})
					    )r ) r)
				      (= :context-switches (key e)) (if-let [l (:context-switches @last-cpu)]
								      (let [seconds (/ (- timestamp @last-time) 1000.0)]
									(assoc r "context switches/s" (/ (- (val e) l) seconds)))
								      r)
				      (= :interrupts (key e)) (if-let [l (:interrupts @last-cpu)]
								(let [seconds (/ (- timestamp @last-time) 1000.0)]
								  (assoc r "interrupts/s" (/ (- (val e) l) seconds)))
								r)
				      :else r))
				   {} new-values)
		    pids (let [total-new (get new-values "Total")
			       total-old (get @last-cpu "Total")]
			   (if total-old
			     (let [{sum :sum} (cpu-jiffies total-new total-old)]
			       
			       (reduce (fn [r e] r
					 (if (= Integer (class (key e)))
					   (if-let [l (get @last-cpu (key e))]
					     (let [user (percent (- (:user (val e)) (:user l)) sum)
						   system (percent (- (:system (val e)) (:system l)) sum)]
					       (assoc r (key e) (assoc {} "user" user "system" system "total" (+ system user))))
					     r)
					   r)) {} new-values))
			     {}))]
		(merge system {"process" pids}))
		     
	      (finally
	       (reset! last-time timestamp)
	       (reset! last-cpu new-values)))))))))

;Have a look at full circle #39
  (defn mem []
    (let [patterns (assoc (sorted-map)
		     :total-mem "MemTotal:";physical total
		     :free-mem "MemFree:";LowFree+HighFree, that is physical free
		     :buffers "Buffers:";Mem in buffer cache
		     :cached "Cached";;Disk cache - swap cache
		     :swap-cache "SwapCached:"; Swaped mem in disk cache
		     :swap-total "SwapTotal:";swap memory
		     :swap-free "SwapFree:";swap free
		     :actve "Active:" ;Memory that has been used more recently and usually not reclaimed unless absolutely necessary.
		     :dirty "Dirty:";Might need writing to disk
		     :total-low "LowTotal:";Total avail for kernel
		     :free-low "LowFree:";Free for kernel, might be occupied by others too
		     :slab "Slab:";Continous Kernel data structure cache
		     :mapped "Mapped:"
		     :write-back "Writeback:"
		     :inactive "Inactive:"
		     )]
      (with-open [stream (BufferedReader. (FileReader. "/proc/meminfo"))]
	(let [values (into {} (for [^String line (line-seq stream)
				    ^String pat patterns
				    :when (.startsWith line (val pat))]
				[(key pat) (Long/parseLong (.trim (.substring line
								        (inc (.length ^String (val pat)))
								       (- (.length line) 3))))]))] 
		       
	  (let [result (atom 
			(assoc {}
			  "physical free" (* 1024 (:free-mem values))
			  "physical used" (* 1024 (- (:total-mem values) (:free-mem values)))
			  "swap used" (* 1024 (- (:swap-total values) (:swap-free values) (:swap-cache values)))
			  "cache" (* 1024 (+ (:cached values) (:swap-cache values)))
			  "% available" (* 100.0 (/ (+ (:free-mem values) (:cached values) (:buffers values) (:swap-cache values))
						    (:total-mem values)))
			  "% cache" (* 100.0 (/ (+ (:cached values) (:swap-cache values))
						(:total-mem values)))
			  "% swaped" (* 100.0 (/ (- (:swap-total values) (:swap-free values) (:swap-cache values))
						 (+ (:swap-total values) (:total-mem values))))
			  "% swap" (* 100.0  (/ (- (:swap-total values) (:swap-free values) (:swap-cache values))
						(:swap-total values)))
			  ))]
	    
	    #_(when (:free-low values)
	      (swap! result #(assoc %   "low free" (* 1024 (:free-low values))
				    "low used" (* 1024 (- (:total-low values) (:free-low values))))))
	    #_(when (:inactive values)
		(swap! result #(assoc %   "inactive" (* 1024 (:inactive values))))
		(swap! result #(assoc %  "active" (* 1024 (:active values))
		)))
	    
	    #_(when (:writeback values)
		(swap! result #(assoc %  "dirty" (* 1024 (+ (:dirty values) (:writeback values))))))
	    #_(when-let [slab (:slab values)]
		(swap! result #(assoc % "slab" (* 1024 (:slab values))))
		(swap! result #(assoc % "buffers" (* 1024 (:buffers values)))))
	    #_(when-let [slab (:mapped values)]
		(swap! result #(assoc % "mapped" (* 1024 (:mapped values)))))
	    
	    (let [processes (assoc {}
			      "process"
			      (reduce (fn [r ^File pid-dir]
					(try
					  (with-open [stat (BufferedReader. (FileReader. (File. pid-dir "smaps")))]
					    (let [data (loop [size 0 shared 0 private 0 resident 0 swapped 0 lines (line-seq stat)]
							 (if-let [line ^String (first lines)]
							 (cond
							  (.startsWith line "Pss:")
							  (recur (+ size (Long/parseLong (second (.split #"\s+" line)))) shared private resident swapped (next lines))
							  (.startsWith line "Shared")
							  (recur size (+ shared (Long/parseLong (second (.split #"\s+" line)))) private resident swapped (next lines))
							  (.startsWith line "Private")
							  (recur size shared (+ private (Long/parseLong (second (.split #"\s+" line)))) resident swapped (next lines))
							  (.startsWith line "Swap")
							  (recur size shared private resident (+ swapped (Long/parseLong (second (.split #"\s+" line)))) (next lines))
							  (.startsWith line "Rss")
							  (recur size shared private (+  resident (Long/parseLong (second (.split #"\s+" line)))) swapped (next lines))
							  :else (recur size shared private resident swapped (next lines))
							  )
							 {"shared" (* 1024 shared) "private" (* 1024 private) "proportional" (* 1024 size) "resident" (* 1024 resident) "swapped" (* 1024 swapped)}))]
					    (assoc r (Integer/parseInt (.getName pid-dir))
						   data)))
					  (catch IOException _
					    r))
					
					) {} @pids-of-interest))]
	      (if-not (empty? (get processes "process"))
		(merge @result processes)
		@result)))))))



(defn to-text [net-dev disk cpu]
  (pr-str (assoc {} (.getHostName (java.net.InetAddress/getLocalHost)) (assoc {} "Network Device" (net-dev) "Disk" (disk) "Cpu" (cpu) "Memory" (mem) "Load" (sys-load) "Descriptors" (fd)))))



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
  ([^Socket socket stop]
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
						      (if (= 5 (count names))
							(zipmap [:host :category :instance :pid :counter] names)							
						      (zipmap [:host :category :counter] names))))]
					 (add-data name timestamp (val e))))
				     d))))
		     data)))
       (catch Exception e
	 (if (.contains (.getMessage e) "EOF" )
	   (info (str "Lost contact with linux proc process at " (.getHostName (.getRemoteSocketAddress  socket))))
	   (if (.contains (.getMessage e) "Stopped")
	     (info "Fetching info from remote linux proc stopped")  
	     (throw e))))))
  ([^String host ^Integer port stop]
     (try 
       (process-remote-linux-proc (java.net.Socket. host port) stop)
       (catch java.net.ConnectException e
	 (info (str "Nobody is listening on " host " port " port))))))
  
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

	    


(defn serve-linux-proc [port frequency ^java.util.regex.Pattern re]
  (if (System/getProperty "java.awt.headless")
    (shutdown-file ".shutdownlinuxprobe")
    (shutdown-button "Monitor Linux proc Agent"))
  (shutdown-jmx "monitor.linuxproc")
  (when re
    (info (str "Looking for processes where command line matches regular expression: " (.pattern re))))
  (let [server (ServerSocket. port)
	net-dev (net-dev-fn)
	disk (disk-fn)
	cpu (cpu-fn)
	sockets (atom #{})
	accept-executor (Executors/newSingleThreadExecutor
			   (proxy [java.util.concurrent.ThreadFactory] []
			     (newThread [^Runnable runnable] (doto (Thread. runnable)
						     (.setDaemon true)))))
	]
    (. accept-executor execute (fn acceptor []
				 (.setSoTimeout server 1000)
				 (while (not (terminating?))
				   (try
				     (if-let [socket (.accept server)]
				       (do (info "Got connection")
					   (.setSoTimeout socket 2000)
					   (swap! sockets (fn [sockets] (conj sockets socket)))))
				     (catch SocketTimeoutException _)))
				 ))
    (.shutdown accept-executor)

    (loop [tim (Date.)]
      (try
      (when re
	(reset! pids-of-interest (interesting-pids re))
	;(doseq [i @pids-of-interest]
	 ; (println (slurp (str (.getAbsolutePath i) "/cmdline"))))
	)
      
      (let [text (to-text net-dev disk cpu)]
	(dorun (map (fn [^Socket s]
		      (try
			(binding [*out* (OutputStreamWriter. (.getOutputStream s))]
			  (println text)
			  (flush))
			(catch Exception e
			  (info (.getMessage e))
			  (info "Connection thrown")
			  (swap! sockets (fn [sockets] (disj sockets s ))))))
		    @sockets)))

      (term-sleep-until tim)
      (catch Exception e
	(.printStackTrace e)))
      (when-not (.isTerminated accept-executor)
	(recur (Date. ^Long (+ (.getTime tim) (* 1000 frequency))))))))
    
			    
