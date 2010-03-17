(ns conf1
  (:use (se.sj.monitor database jmxcollection server clientif logger))
  (:use [se.sj.monitor.perfmonservice :only (perfmon-connection)]))

(using-logger
 (using-history "/home/stefan/testdb"
		(let [interval 15]
		  (serve-clients 0 3030 #(deref stop-signal) 
				
				 (serve [(fn [] (jmx-java6 (fn [] 
							     (when (not @stop-signal)
							       (try
								(Thread/sleep (* interval 1000))
								(catch InterruptedException e)))
							     @stop-signal)))
					 (perfmon-connection "mssj022" 3434 (fn []  @stop-signal) ".*" "Processor" ".*User Time" ".*")
					 
					 (fn [] (while (not @stop-signal)
						       (try (Thread/sleep 30000)
							    (catch InterruptedException _))
						       (clean-live-data-older-than 
							(java.util.Date. (- (System/currentTimeMillis) 
									    (* 1000 60 60))))))
					 ])))
		(clean-live-data-older-than (. (java.text.SimpleDateFormat. "yyyyMMdd") parse "19700101"))))