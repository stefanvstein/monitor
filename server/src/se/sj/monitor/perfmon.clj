(ns se.sj.monitor.perfmon 
  (:use clojure.test)
  (:import (java.text SimpleDateFormat ParseException)))

(def perfmon-columns (list :host :category :counter :instance))
(defn create-adress-and-value [line]
  (when-let [line-match (re-matches #"([0-9]+(\.[0-9]+){0,3}) (.*)" line)]
    (when-let [value (last line-match)]
      (when-let [address (reduce 
			  #(assoc %1 (first %2) (Integer/parseInt (second %2))) 
			  {} 
			  (map #(list %1 %2) 
			       '(:host :category :counter :instance) 
			       (re-seq #"[0-9]+" (second line-match))))]
	{:address address :value value}))))

 
(defn parse-perfmon-string [line onStructure onData onInfo onTime]
  (when-let [line-match (re-matches #"([DCTI])\s(.+)" line)]
    (let [type (second line-match),
	  value (last line-match)]
      (try
       (condp = type 
	 "C" (when-let [s (create-adress-and-value value )] 
	       (onStructure (:address s) (:value s)))
	 "D" (when-let [s (create-adress-and-value value)]
	       (onData (:address s) (Double/parseDouble(:value s))))
	 "I"  (onInfo value)
	 "T"  (when-let [v (re-matches #"[0-9]{8}\s[0-9]{6}\s.+" value)]
		(onTime (.parse (SimpleDateFormat. "yyyyMMdd HHmmss z") (.replace v ":" "")))))
      (catch ParseException _)
      (catch NumberFormatException _)))))

(deftest test-perfmon
  (is (= (list {:counter 2, :category 1, :host 0} 34) 
	 (parse-perfmon-string "D 0.1.2 34" 
			       nil 
			       (fn [adr val] (list adr val)) 
			       nil 
			       nil))
    "Data line")
  (is (= (list {:counter 2, :category 1, :host 0 :instance 4} "34")
	 (parse-perfmon-string "C 0.1.2.4 34" 
			       (fn [adr val] (list adr val))
			       nil
			       nil
			       nil))

    "Create line")
  (is (= "Olle was here" 
	 (parse-perfmon-string "I Olle was here" 
			       nil
			       nil
			       (fn [com] com)
			       nil))
    "Info line")
  (let [df (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss") ]
	(is (= (. df parse "2007-01-27 12:34:16" )
	       (parse-perfmon-string "T 20070127 123416" nil nil nil (fn [t] t)))
	    "time line")))
