(ns monitor.jmxthreads
  (:use [monitor.jmxcollection :only [*add* jmx-threads]])
  (:use [monitor.termination :only [term-sleep]])
  (:import (java.io PrintWriter FileWriter))
  )

(defn read-threads
  ([name jmx-host jmx-port stop-fn]
     (throw (UnsupportedOperationException. "Hammarbynisse")))
  ([name jmx-host jmx-port file seconds-per-sample]
     (let [port (if (instance? String jmx-port)
		  (Integer/valueOf jmx-port)
		  jmx-port)
	   seconds (if (instance? String seconds-per-sample)
		  (Integer/valueOf seconds-per-sample)
		  seconds-per-sample)]
     (with-open [out (PrintWriter. (FileWriter. file) true)]
       (binding [*add* (fn [names time-stamp value]
			 (.println out (pr-str [names (.getTime time-stamp) value])))]
	 (println "Calling jmx-threads " name jmx-host port seconds)
	 (jmx-threads name jmx-host port (fn [] (term-sleep seconds)))))))
  ([name jmx-host jmx-port client-host client-port stop-fn]
     (throw (UnsupportedOperationException. "Ansiktsburk")))
  ([args]
     (apply read-threads args)))


(defn import-threads-file [file client-host client-port])
  