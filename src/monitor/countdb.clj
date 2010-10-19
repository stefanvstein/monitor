(ns monitor.countdb
  (:import [java.lang.management ManagementFactory])
  (:import [javax.management ObjectName])
  (:import [java.text SimpleDateFormat])
  (:use [monitor db]))
(defn jmx-db-record-counter [text]
(gen-interface :name monitor.CountDBRecordsMXBean
	       :methods [[count [String] long]])
 (.registerMBean (ManagementFactory/getPlatformMBeanServer)
		  (proxy [monitor.CountDBRecordsMXBean] []
		    (count [date-string] 
			   (let [date (.parse (SimpleDateFormat. "yyyyMMdd") date-string)]
			     (try
			       (records date)
			       (catch Exception e
				 (.printStackTrace e)))
				)))
		  (ObjectName. (str text ":type=Tools"))))