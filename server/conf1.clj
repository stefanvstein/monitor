(ns conf1
  (:use (se.sj.monitor database jmxcollection server clientif logger))
  (:use [se.sj.monitor.perfmonservice :only (perfmon-connection)]))

(defn sleepSeconds [interval stop-signal]
  (when (not @stop-signal)
    (try
     (Thread/sleep (* interval 1000))
     (catch InterruptedException e))))

(using-logger
 (using-live
 (using-history "/home/stefan/testdb"
		(let [interval 15]
		  (serve-clients 0 3030 #(deref stop-signal) 
				
				 (serve [(fn java6local [] (jmx-java6 (fn sleep [] 
						     (sleepSeconds interval stop-signal)
						     @stop-signal)))
		;			 (fn java6Java2D [] (jmx-java6 "Java2D" 
		;					  "ladmi2338" 
		;					  3031 
		;					  (fn sleep [] 
		;					    (sleepSeconds interval stop-signal)
		;					    @stop-signal)))

		;			 (perfmon-connection "mssj022" 3434 (fn []  @stop-signal) 
		;					     ".*" 
		;					     "Processor|System|Memory|PhysicalDisk|Network Interface" 
		;					     ".*User Time|.*Interrupt Time|.*Processor Time|.*Idle Time|Interrupts/sec|Context Switches/sec|Available Bytes|Current Disk Queue Length|Output Queue Length" ".*")
		;			 (perfmon-connection "oxgroda" 3434 (fn []  @stop-signal) ".*" "Processor" ".*User Time" ".*")
					 (fn clean-live [] (while (not @stop-signal)
						       (try (Thread/sleep 30000)
							    (catch InterruptedException _))
						       (clean-live-data-older-than 
							(java.util.Date. (- (System/currentTimeMillis) 
									    (* 1000 60 60))))))
					 (fn clean-stored [] (while (not @stop-signal)
						       (try (Thread/sleep (* 1000 60 3 ))
							    (catch InterruptedException _))
						       (clean-stored-data-older-than 
							(java.util.Date. (- (System/currentTimeMillis) 
									    (* 2 1000 60 60))))))

					 ]))))))
