(ns monitor.main
  (:gen-class)
  (:use (monitor gui)))
 
(defn -main [& line]
  (when (not (= "true" (System/getProperty "stayalive")))
    (reset! exit-on-shutdown true))
  (if (not (empty? line))
    (when (= (first line) "server")
      (System/setProperty "JEMonitor" "true")
      (if (next line)
	(let [filename (if (re-find #"(?i).+\.clj$" (second line))
			 (second (re-find #"(?i)(.+)\.clj$" (second line)))
			 (second line))]
	     
	  (do (load (str "/" filename)))
	  (load "/conf1"))))
      (new-window true)))
