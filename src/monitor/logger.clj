(ns monitor.logger
  (:import (java.util.logging Logger SimpleFormatter ConsoleHandler Formatter Level))
  (:use (clojure stacktrace test))
  (:use [clojure.contrib.logging :only (error info)] )
  )

(defn use-logger []
  (let [logger (Logger/getLogger "monitor")
	 handler (let [ han (ConsoleHandler.)
			lastHead (atom "")]
			(. han setFormatter 
			   (proxy [Formatter] []
			     (getHead [h] "")
			     (getTail [h] "")
			     (format [logRecord]
				     (let [df (java.text.SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")
					   thisHead 
					   (with-out-str 
					     (print (str 
						     (.format df (java.util.Date. (.getMillis logRecord)))
						     " " 
						     (.getLoggerName logRecord))))]
				       (with-out-str
					 (when-not (= @lastHead thisHead)
					   (do 
					     (println thisHead)
					     (swap! lastHead (fn [_] thisHead))))
					 (println (str "  " 
						       (. logRecord getLevel) 
						       ": " 
						       (.getMessage logRecord)))
					 (when-let [ex (.getThrown logRecord)]
					   (print-cause-trace ex)
					   (swap! lastHead (fn [_] "")))))))) 
			han)]
	  (. logger addHandler handler)
	  (. logger setLevel Level/FINEST)
	  (. handler setLevel Level/FINEST)
	  (. logger setUseParentHandlers false)
	  (fn [] (. logger removeHandler handler))))
  
(defmacro using-logger
  [& form]
  `(let [logger# (Logger/getLogger "monitor")
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

(deftest try-some-logging
    (using-logger
     (info "I am the myth")
    (info "Ooops... This is thrown by intention" (IllegalStateException. "This is it"))
    ))