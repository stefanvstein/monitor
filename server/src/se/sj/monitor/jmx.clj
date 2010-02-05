(ns se.sj.monitor.jmx
  (:use (se.sj.monitor mem names) )
  (:use [clojure.contrib.duck-streams :only (reader)] )
  (:import (javax.management.remote JMXConnectorFactory JMXServiceURL) 
	   (javax.management JMX ObjectName) 
	   (java.lang.management RuntimeMXBean)
	   (java.util Date)
	   (java.text SimpleDateFormat))
					; for testing purposes
  (:use clojure.test)
					;for repl purposes
  (:use (clojure stacktrace inspector)))

(defn tryit []
  (with-open [connector (. JMXConnectorFactory connect 
			   (JMXServiceURL. "service:jmx:rmi:///jndi/rmi://localhost:3333/jmxrmi"))]
    (let [connection (. connector getMBeanServerConnection)
	  runtime (. JMX newMXBeanProxy connection (ObjectName. "java.lang:type=Runtime") RuntimeMXBean)]
      (str (. runtime getName) 
	   " " 
	   (Date. (+ (. runtime getStartTime) (. runtime getUptime))) 
	   "   " (. runtime getInputArguments))
)))

  

