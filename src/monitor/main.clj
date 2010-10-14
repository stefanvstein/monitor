(ns monitor.main
  (:gen-class)
  (:use (monitor logger)))
 
(defn -main [& line]
  (using-logger
  
  (if (not (empty? line))
    (do
      (when (= (first line) "server")
	(System/setProperty "JEMonitor" "true")
	(if (next line)
	  (let [filename (if (re-find #"(?i).+\.clj$" (second line))
			   (second (re-find #"(?i)(.+)\.clj$" (second line)))
			   (second line))]
	    
	    (load (str "/" filename)))
	    (println "You need to supply a config file" )))
      (when (= (first line) "linux")
	(if (next line)
	  (if-let [port (Integer/parseInt (second line))]
	    (do
	      (def listener-port port)
	      (load "/monitor/linuxproc")
	      (eval '(monitor.linuxproc/serve-linux-proc monitor.main/listener-port 15)))
	    (println "You need to supply a listener port")))))
    (do
      (load "/monitor/gui")
      (eval '(monitor.gui/new-window true))))))
