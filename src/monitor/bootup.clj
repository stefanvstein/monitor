(ns monitor.bootup
  (:use (monitor logger linuxproc gui)))

(defn boot [line]
  (using-logger
   (if-not (empty? line)
     (do (when (= (first line) "server")
	   (System/setProperty "JEMonitor" "true")
	   (if (next line)
	     (let [filename (if (re-find #"(?i).+\.clj$" (second line))
			      (second (re-find #"(?i)(.+)\.clj$" (second line)))
			      (second line))]
	       (load (str "/" filename)))
	     (println "You need to supply a config file" )))
	 (when (= (first line) "linux")
	   (if (next line)
	     (serve-linux-proc (Integer/parseInt (second line)) 15)
	     (println "You need to supply a listener port"))))
     (new-window true))))