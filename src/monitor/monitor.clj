(ns monitor.monitor
  (:use (monitor database jmxcollection server clientif logger linuxproc shutdown termination))
  (:use [monitor.perfmonservice :only (perfmon-connection)]))

(defn java6-jmx
  ([]
     (dosync (alter tasks-to-start conj (fn java6-jmx-local [] (jmx-java6 (fn [] (term-sleep 15)))))))
  ([name host port]
     (dosync (alter tasks-to-start conj (fn java6-jmx-remote [] (jmx-java6 name host port (fn [] (term-sleep 15 ))))))))

(defn perfmon
  ([host port hosts-expression categories-expression counters-expression instances-expression]
     (dosync (alter tasks-to-start conj (perfmon-connection host port terminating? hosts-expression categories-expression counters-expression instances-expression))))
  ([host port categories-expression counters-expression instances-expression]
     (dosync (alter tasks-to-start conj (perfmon-connection host port terminating? ".*" categories-expression counters-expression instances-expression))))
  ([host port counters-expression instances-expression]
     (dosync (alter tasks-to-start conj (perfmon-connection host port terminating? ".*" ".*" counters-expression instances-expression)))))

(defn linux-proc [host port]
  (dosync (alter tasks-to-start conj #(process-remote-linux-proc host port terminating?))))

(defn in-env [ live-minutes history-days history-directory client-port]
  (when (not (System/getProperty "nogui"))
    (shutdown-button "Monitor server"))
  (shutdown-jmx "monitor.server")
   (using-live
    (using-history history-directory
		   (serve-clients 0 client-port terminating?
				  (dosync alter tasks-to-start conj
					  (fn live-cleaner []
					    (while (not (terminating?))
					      (try (term-sleep 30)
						   (catch InterruptedException _#))
					      (when-not (terminating?)
					      (clean-live-data-older-than 
					       (java.util.Date. (- (System/currentTimeMillis) 
								   (* 1000 60 live-minutes)))))))
					  (fn history-cleaner []
					    (while (not (terminating?))
						       (try (term-sleep (* 60 3 ))
							    (catch InterruptedException _))
						       (when-not (terminating?)
						       (clean-stored-data-older-than 
							(java.util.Date. (- (System/currentTimeMillis) 
									    (* history-days 24 1000 60 60))))))))
				  (serve)
				  )
		   ))
   (println "Done"))					    
  


  
  