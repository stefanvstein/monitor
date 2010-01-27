(ns se.sj.monitor.perfmon 
 ; (:gen-class)
  (:import (java.text SimpleDateFormat ParseException)))

(defn create-adress-and-value [line]
  (when-let [line-match (re-matches #"([0-9]+(\.[0-9]+){0,3}) (.*)" line)]
    (when-let [value (last line-match)]
      (when-let [address (reduce 
			  #(assoc %1 (first %2) (Long/parseLong (second %2))) 
			  {} 
			  (map #(list %1 %2) 
			       '(:host :category :counter :instance) 
			       (re-seq #"[0-9]+" (second line-match))))]
	{:address address :value value}))))

(defn parse-perfmon-string [line onStructure onData onInfo, onTime]
  (when-let [line-match (re-matches #"([DCTI])\s(.+)" line)]
    (let [type (second line-match),
	  value (last line-match)]
      (try
       (condp = type 
	 "C" (when-let [s (create-adress-and-value value)] 
	       (onStructure (:address s) (:value s)))
	 "D" (when-let [s (create-adress-and-value value)]
	       (onData (:address s) (Double/parseDouble(:value s))))
	 "I"  (onInfo value)
	 "T"  (when-let [v (re-matches #"[0-9]{8}\s[0-9]{6}" value)]
		(onTime (.parse (SimpleDateFormat. "yyyyMMdd HHmmss") v))))
      (catch ParseException _)
      (catch NumberFormatException _)))))