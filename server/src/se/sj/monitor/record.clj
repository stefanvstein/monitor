(ns se.sj.monitor.record
;(:gen-class)
(:use (se.sj.monitor mem perfmon))
(:import (java.io IOException BufferedReader FileReader InputStreamReader PrintStream) 
	 (java.net Socket SocketException )))

(defn close-in [closeable time] 
(Thread/sleep time)
(. closeable close))

(defn letsdoit []
  (try
   (with-open [socket (Socket. "mssj022" 3434)]
     (when-let [input (BufferedReader. (InputStreamReader. (. socket getInputStream)))]
       (when-let [output (PrintStream. (. socket getOutputStream) true)]
	 
	 (. output println "start")
;	 (. output flush)	
;	 (println "we have two streams")
	 
	 (. (Thread. #(close-in input 20000)) start)
;	 (println "Thread started")
	 (dorun (map #(println %) 
		     (line-seq input)))
	 )))
  (catch Exception e (println (. e getMessage)))))
(letsdoit)
