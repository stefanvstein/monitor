(ns monitor.monitor
  (:use [monitor.database :only [using-live
				 using-history
				 clean-live-data-older-than
				 sync-db
				 clean-stored-data-older-than
				 compress-older-than]])
    (:use [clojure.contrib.logging :only [info]])
  (:use [monitor.jmxcollection :only [jmx-java6]])
  (:use [monitor.server :only [tasks-to-start
			       serve]])
  (:use [monitor.clientif :only [serve-clients]])
  (:use [monitor.linuxproc :only [process-remote-linux-proc]])
  (:use [monitor.shutdown :only [shutdown-file
				 shutdown-button
				 shutdown-jmx]])
  (:use [monitor.termination :only [terminating?
				    term-sleep]])
  (:use [monitor.countdb :only [jmx-db-record-counter]])
  (:use [monitor.perfmonservice :only (perfmon-connection)])
  (:use [monitor.jmxremote :only [jmx-remote]]))

(defn java6-jmx
  ([]
     (dosync (alter tasks-to-start conj (fn java6-jmx-local []
					  (binding [monitor.jmxcollection/*add* monitor.database/add-data]
					    (jmx-java6 (fn [] (term-sleep 15))))))))
  ([name host port]
     (dosync (alter tasks-to-start conj (fn java6-jmx-remote []
					  (binding [monitor.jmxcollection/*add* monitor.database/add-data]
					    (jmx-java6 name host port (fn [] (term-sleep 15 )))))))))

(defn perfmon
  ([host port hosts-expression categories-expression counters-expression instances-expression]
     (dosync (alter tasks-to-start conj (perfmon-connection host
							    port
							    terminating? '
							    hosts-expression
							    categories-expression
							    counters-expression
							    instances-expression))))
  ([host port categories-expression counters-expression instances-expression]
     (dosync (alter tasks-to-start conj (perfmon-connection host
							    port
							    terminating?
							    ".*"
							    categories-expression
							    counters-expression
							    instances-expression))))
  ([host port counters-expression instances-expression]
     (dosync (alter tasks-to-start conj (perfmon-connection host
							    port
							    terminating?
							    ".*"
							    ".*"
							    counters-expression
							    instances-expression)))))

(defn linux-proc [host port]
  (dosync (alter tasks-to-start conj #(process-remote-linux-proc host
								 port
								 terminating?))))
(defn remote-jmx [host port]
  (dosync (alter tasks-to-start conj #(jmx-remote host port terminating?))))

(defn in-env 
  ([live-minutes history-days history-directory client-port client-transfer-port]
     (if  (System/getProperty "java.awt.headless")
       (shutdown-file ".shutdownmonitor")
       (shutdown-button "Monitor server"))
     (shutdown-jmx "monitor.server")
     (using-live
      (dosync (alter tasks-to-start conj
		     (fn live-cleaner []
		       (while (not (terminating?))
			 (try (term-sleep 30)
			      (catch InterruptedException _#))
			 (when-not (terminating?)
			   (clean-live-data-older-than 
			    (java.util.Date. (- (System/currentTimeMillis) 
						(* 1000 60 live-minutes)))))))))
      
      (using-history history-directory
		     (jmx-db-record-counter "monitor.server")
		     
		     (dosync
		      (alter tasks-to-start conj
			     (fn syncer []
			       (while (not (terminating?))
				 (try (term-sleep (* 1 60))
				      (catch InterruptedException _))
				 (sync-db))))
		      
		      (alter tasks-to-start conj
			     (fn history-cleaner []
			       (while (not (terminating?))
				 (try (term-sleep (* 60 60 3 ))
				      (catch InterruptedException _))
				 (when-not (terminating?)
				   (clean-stored-data-older-than 
				    (java.util.Date. (- (System/currentTimeMillis) 
							(* history-days 24 1000 60 60))))
				   (compress-older-than
				    (java.util.Date.)
				    terminating?))))))
		     
		     (serve-clients client-transfer-port client-port terminating?
				    (serve))))
     (info "Done"))
  ([history-days history-directory client-port jmx-port]
     (in-env 1 history-days history-directory client-port jmx-port))
  ([history-days history-directory client-port]
     (in-env 1 history-days history-directory client-port 0)))



  
  