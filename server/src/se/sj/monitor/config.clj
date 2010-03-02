(ns se.sj.monitor.config
  (:use (se.sj.monitor database jmxcollection server))
  (:use [se.sj.monitor.perfmonservice :only (perfmon-connection)]))




(defn nr1 
  []
  (let [interval 15
	perfmon-stop-signal (ref (fn [] @stop-signal))
	perfmon-comments (atom {})] 
    
  (serve [(fn [] (jmx-java6 (fn [] 
			      (when (not @stop-signal)
				(try
				 (Thread/sleep (* interval 1000))
				 (catch InterruptedException e)))
			      @stop-signal)))
	  (perfmon-connection perfmon-comments "mssj022" 3434 perfmon-stop-signal ".*" "Processor" ".*User Time" ".*")
])))
				   
			

