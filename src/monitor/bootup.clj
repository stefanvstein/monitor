(ns monitor.bootup
  (:use (monitor logger linuxproc gui jmxremote jmxthreads)))

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
	   (if-let [port (second line)]
	     (serve-linux-proc (Integer/parseInt port) 15 (if-let [pat (get (vec line) 2)]
							     (re-pattern pat)
							     nil))
	     
	     (println "You need to supply a listener port")))
	 (when (= "jmx" (first line))
	   (jmx-probe (rest line)))
	 (try
	 (when (= "threadsfile" (first line))
	   (apply read-threads-to-file (rest line)))
	 (when (= "threads" (first line))
	   (apply read-threads (rest line)))
	 (when (= "importthreadsfile" (first line))
	   (apply import-threads-file (rest line)))
	 (catch IllegalArgumentException e
	   (println (.getMessage e))))
	 (when (= "gui" (first line))
	   (new-window true (second line) (nth line 2)))) 
	   
     (new-window true))))
