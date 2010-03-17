(ns se.sj.monitor.main
  (:gen-class)
  (:use (se.sj.monitor gui)))
 
(defn -main [& line]
  (println line)
  (if (not (empty? line))
    (when (= (first line) "server")
      (if (next line) 
	(load (str "/" (second line)))
	(load "/conf1")))
    (new-window true)))
