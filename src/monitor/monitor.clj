(ns monitor.monitor
  (:use (monitor database jmxcollection server clientif logger))
  (:use [monitor.perfmonservice :only (perfmon-connection)]))

(defn- sleepSeconds [interval]
  (when (not @stop-signal)
    (try
     (Thread/sleep (* interval 1000))
     (catch InterruptedException e))))


(defn java6-jmx
  ([]
     (dosync (alter tasks-to-start conj (fn java6-jmx-local [] (jmx-java6 (fn [] (sleepSeconds 15)))))))
  ([name host port]
     (dosync (alter tasks-to-start conj (fn java6-jmx-remote [] (jmx-java6 name host port (fn [] (sleepSeconds 15 ))))))))

(defn perfmon
  ([host port hosts-expression categories-expression counters-expression instances-expression]
     (dosync (alter tasks-to-start conj (perfmon-connection host port (fn []  @stop-signal) hosts-expression categories-expression counters-expression instances-expression))))
  ([host port categories-expression counters-expression instances-expression]
     (dosync (alter tasks-to-start conj (perfmon-connection host port (fn []  @stop-signal) ".*" categories-expression counters-expression instances-expression))))
  ([host port counters-expression instances-expression]
     (dosync (alter tasks-to-start conj (perfmon-connection host port (fn []  @stop-signal) ".*" ".*" counters-expression instances-expression)))))

(defn in-env [ live-minutes history-days history-directory client-port]
   (using-live
    (using-history history-directory
		   (serve-clients 0 client-port #(deref stop-signal)
				  (dosync alter tasks-to-start conj
					  (fn live-cleaner []
					    (while (not @stop-signal)
					      (try (Thread/sleep 30000)
						   (catch InterruptedException _#))
					      (clean-live-data-older-than 
					       (java.util.Date. (- (System/currentTimeMillis) 
								   (* 1000 60 live-minutes))))))
					  (fn history-cleaner []
					    (while (not @stop-signal)
						       (try (Thread/sleep (* 1000 60 3 ))
							    (catch InterruptedException _))
						       (clean-stored-data-older-than 
							(java.util.Date. (- (System/currentTimeMillis) 
									    (* history-days 24 1000 60 60)))))))
				  (serve)))))					    
  


  
  