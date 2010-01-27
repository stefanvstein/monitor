(ns se.sj.monitor.server
;(:gen-class)
(:use (se.sj.monitor mem perfmon))
(:import (java.io BufferedReader FileReader)))
(defn testme []
  (def data (ref {}))
  (with-open [input (BufferedReader. (FileReader. "test.txt"))]
    (dorun(map #(println %) (line-seq input)))
    ))

