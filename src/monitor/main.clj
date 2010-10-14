(ns monitor.main
  (:gen-class))
 
(defn -main [& line]
  (def line line)
  (load "/monitor/bootup")
  (eval '(monitor.bootup/boot monitor.main/line)))
 
