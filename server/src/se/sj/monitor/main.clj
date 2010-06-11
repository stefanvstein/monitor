(ns se.sj.monitor.main
  (:gen-class)
  (:use (se.sj.monitor gui)))
 
(defn -main [& line]
  (System/setProperty "JEMonitor" "true")
  (reset! exit-on-shutdown true)
  (if (not (empty? line))
    (when (= (first line) "server")
      (if (next line) 
	(do (load (str "/" (second line))))
	(load "/conf1")))
    (new-window true)))
