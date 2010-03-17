(ns se.sj.monitor.logger
  (:import (java.util.logging Logger SimpleFormatter ConsoleHandler Formatter Level))
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