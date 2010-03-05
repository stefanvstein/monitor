(ns se.sj.monitor.config
  (:import (java.util.logging Logger SimpleFormatter ConsoleHandler Formatter Level))
  (:use (se.sj.monitor database jmxcollection server))
  (:use [se.sj.monitor.perfmonservice :only (perfmon-connection)])
  (:use (clojure stacktrace)))


(defmacro using-logger
  [& form]
  `(let [logger# (Logger/getLogger "se.sj.monitor")
	 handler# (let [ han# (ConsoleHandler.)
			lastHead# (atom "")]
			(. han# setFormatter 
			   (proxy [Formatter] []
			     (getHead [h#] "")
			     (getTail [h#] "")
			     (format [logRecord#]
				     (let [df# (java.text.SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")
					   thisHead# 
					   (with-out-str 
					     (print (str 
						     (.format df# (java.util.Date. (.getMillis logRecord#)))
						     " " 
						     (.getLoggerName logRecord#))))]
				       (with-out-str
					 (when-not (= @lastHead# thisHead#)
					   (do 
					     (println thisHead#)
					     (swap! lastHead# (fn [_#] thisHead#))))
					 (println (str "  " 
						       (. logRecord# getLevel) 
						       ": " 
						       (.getMessage logRecord#)))
					 (when-let [ex# (.getThrown logRecord#)]
					   (print-cause-trace ex#)
					   (swap! lastHead# (fn [_#] "")))))))) 
			han#)]
	  (. logger# addHandler handler#)
	  (. logger# setLevel Level/FINEST)
	  (. handler# setLevel Level/FINEST)
	  (. logger# setUseParentHandlers false)
	  (try
	   (do ~@form)
	  (finally (. logger# removeHandler handler#)))))

(defn nr1 
  []
  (using-logger
  (let [interval 15]
    
    (serve [(fn [] (jmx-java6 (fn [] 
				(when (not @stop-signal)
				  (try
				   (Thread/sleep (* interval 1000))
				   (catch InterruptedException e)))
				@stop-signal)))
	    (perfmon-connection "mssj022" 3434 (fn []  @stop-signal) ".*" "Processor" ".*User Time" ".*")
;	    (perfmon-connection "mssj022" 3437 (fn []  @stop-signal) ".*" "Processor" ".*User Time" ".*")
	    (fn [] (while (not @stop-signal)
			  (try (Thread/sleep 30000)
			       (catch InterruptedException _))
			  (clean-live-data-older-than 
			   (java.util.Date. (- (System/currentTimeMillis) 
						(* 1000 60 60))))))
	    ]))))
				   
			

