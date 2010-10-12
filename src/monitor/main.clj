(ns monitor.main
  (:gen-class)
  (:use (monitor gui logger linuxproc)))
 
(defn -main [& line]
  (using-logger
  (when (not (= "true" (System/getProperty "stayalive")))
    (reset! exit-on-shutdown true))
  (if (not (empty? line))
    (do
      (when (= (first line) "server")
	(System/setProperty "JEMonitor" "true")
	(if (next line)
	  (let [filename (if (re-find #"(?i).+\.clj$" (second line))
			   (second (re-find #"(?i)(.+)\.clj$" (second line)))
			   (second line))]
	    
	    (do (load (str "/" filename)))
	    (load "/conf1"))))
      (when (= (first line) "linux")
	(if (next line)
	  (serve-linux-proc (Integer/parseInt (second line)) 15)
	  (println "You need to supply a listener port"))))
    (new-window true))))
