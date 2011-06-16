(ns jmeter.import
  (:use clojure-csv.core)
  (:use clojure.contrib.pprint)
  (:import (java.io PrintWriter FileWriter))
  (:import (com.csvreader CsvReader)) )




(defn- record-seq [filename]
  (let [csv (CsvReader. filename)
        read-record (fn [] 
                      (when (.readRecord csv) 
                        (into [] (.getValues csv))))]
    (take-while (complement nil?) (repeatedly read-record))))

(defn csv-seq
  "Return a lazy sequence of records (maps) from CSV file.

  With header map will be header->value, otherwise it'll be position->value."
  ([filename] (csv-seq filename false))
  ([filename headers?]
   (let [records (record-seq filename)
         headers (if headers? (first records) (range (count (first records))))]
     (map #(zipmap headers %) (if headers? (rest records) records)))))


(defn parse-result
  ([file testcase]
     (parse-result file testcase nil))
  ([file testcase port]
     (parse-result file testcase "localhost" port))
  ([file testcase host port]

     
     (let [connection (when port
			(with-open [s (java.net.Socket. "localhost" port)
				    ois (java.io.ObjectInputStream. (.getInputStream s))]
			  (.readObject ois)))
	   
	   tformat (let [df (java.text.SimpleDateFormat. "HH:mm:ss")]
		     #(.format df %))
	   dateformat (let [df (java.text.SimpleDateFormat. "yyyy-MM-dd")]
		     #(.format df %))
	   
	   dformat (let [df (java.text.DecimalFormat. "##0.###")]
		     #(.format df %))
					;transforms a csv row into a keyed map
	   into-data (fn [values]
		       {:ended (java.util.Date. (Long/parseLong (get values "timeStamp")))
			:timestamp (java.util.Date. (- (Long/parseLong (get values "timeStamp"))
						     (Long/parseLong (get values "elapsed"))))
			:response-time (Long/parseLong (get values "elapsed"))
			:first-packet (Long/parseLong (get values "Latency"))
			:label (get values "label")
			:success  (Boolean/valueOf (get values "success"))
			:thread (get values "threadName")
			:bytes (Long/parseLong (get values "bytes"))})
					;takes a seq of keyed maps and puts them in a map of label key
	   per-trans-name (fn [values]
			    (reduce (fn [r v]
				      (let [label (:label v)]
					(if-let [d (get r label)]
					  (assoc r label (conj d v))
                                          (assoc r label [v])
					  #_(assoc r label (sorted-set-by #(- (.getTime (:timestamp %1))
									    (.getTime (:timestamp %2))) v))))) 
				    {} values))
	   
	   per-second (fn [s] (reduce (fn [result value]
					(let [second (java.util.Date. (* 1000 (long (/ (.getTime (:timestamp value)) 1000))))]
					  (if-let [v (get result second)]
					    (assoc result second (conj v value))
					    (assoc result second #{value})))
					) (sorted-map) s))
	   per-second-statistics (fn [v] 
				   (reduce (fn [r s]
					     (let [tps (count (val s))
						   average-response (/ (/ (apply + (map #(:response-time %) (val s))) tps) 1000.0)
						   max-response (/ (apply max (map #(:response-time %) (val s))) 1000.0)
						   min-response (/ (apply min (map #(:response-time %) (val s))) 1000.0)
						   mean-response 
						   (/ (nth (map #(:response-time %)
									      
								(sort #(int (- (* 1000 (:response-time %1))
									       (* 1000 (:response-time %2))))
								 (val s)))
							   (/ tps 2))
						      1000.0)
						   errors (count (filter #(not (:success %)) (val s)))]

					       (assoc r (key s) {:tps tps
								 :average-response average-response
								 :max-response max-response
								 :min-response min-response
								 :mean-response mean-response
								 :errors errors})))
					   (sorted-map) v))
	   ;Comparator based on :timestamp wich should be a java.util.Date
	   per-time-stamp #(long (- (* 1000 (.getTime (:timestamp %1))) (* 1000 (.getTime (:timestamp %2)))))]

       (let [filename (str testcase "-Requests.csv")]
	 (println "Creating" filename)
	 (with-open [p (PrintWriter. (FileWriter. filename) true)]
	   (.println p "date,timestamp,responsetime,success,user,bytes,request")
	   
	   (doseq [s (sort per-time-stamp (map into-data (csv-seq file true)))]
	     (.println p (str
			  (dateformat (:timestamp s)) ","
			  (tformat (:timestamp s)) ","
			  (dformat (/ (:response-time s) 1000.0)) ","
			  (:success s) ","
			  (:thread s) ","
			  (:bytes s) ","
			  (:label s))))))
		  
		 

       (let [filename (str testcase "-Requests-per-s.csv")]
	 (println "Createing" filename)
	 (with-open [p  (PrintWriter. (FileWriter. filename) true)]
	   (.println p "date,timestamp,tps,average,max,min,mean,errors")
	   (let [vs (reduce (fn [r s]
			      (let [va (val s)
				    ke (key s)]
				(.println p (str
					     (dateformat ke) ","
					     (tformat ke) ","
					     (str (:tps va) ","
						  (dformat (:average-response va)) ","
						  (dformat (:max-response va)) ","
						  (dformat (:min-response va)) ","
						  (dformat (:mean-response va)) ","
						  (:errors va))))
				(-> r
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "tps" "section" "Total"} ke] (:tps va))
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "average" "section" "Total"} ke] (:average-response va))
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "max" "section" "Total"} ke] (:max-response va))
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "min" "section" "Total"} ke] (:min-response va))
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "mean" "section" "Total"} ke] (:mean-response va))
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "errors" "section" "Total"} ke] (:errors va)))
				
				))
				
				
			    {{"instance" testcase "category" "Test" "counter" "tps" "section" "Total"} (sorted-map)
			     {"instance" testcase "category" "Test" "counter" "average" "section" "Total"} (sorted-map)
			     {"instance" testcase "category" "Test" "counter" "max" "section" "Total"} (sorted-map)
			     {"instance" testcase "category" "Test" "counter" "min" "section" "Total"} (sorted-map)
			     {"instance" testcase "category" "Test" "counter" "mean" "section" "Total"} (sorted-map)
			     {"instance" testcase "category" "Test" "counter" "errors" "section" "Total"} (sorted-map)
			     }
			    
			    (per-second-statistics
			     (per-second
			      (sort per-time-stamp (map into-data (csv-seq file true))))))]
	
	     (when connection
	       (doseq [v vs]
		 (.add connection (key v) (val v)))))))
	 
                                        ;(doseq [t (per-trans-name (sort per-time-stamp (map into-data (csv-seq file true))))]
       (doseq [t (per-trans-name (map into-data (csv-seq file true)))]
	 (let [test-name (key t)
	       filename (str testcase "-Requests-per-s-" (key t) ".csv")]
	   (println "Creating" filename)
	 (with-open [p (PrintWriter. (FileWriter. filename) true)]
	   (.println p "date,timestamp,tps,average,max,min,mean,errors")

	   (let [vs (reduce (fn [r s]
			      (let [va (val s)
				  ke (key s)]
				(.println p (str
					     (dateformat ke) ","
					     (tformat ke) ","
					     (str (:tps va) ","
						  (dformat (:average-response va)) ","
						  (dformat (:max-response va)) ","
						  (dformat (:min-response va)) ","
						  (dformat (:mean-response va)) ","
						  (:errors va))))
				(-> r
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "tps" "section" test-name} ke] (:tps va))
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "average" "section" test-name} ke] (:average-response va))
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "max" "section" test-name} ke] (:max-response va))
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "min" "section" test-name} ke] (:min-response va))
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "mean" "section" test-name} ke] (:mean-response va))
				    (assoc-in [{"instance" testcase "category" "Test" "counter" "errors" "section" test-name} ke] (:errors va)))
			    	
			      ))
			    {{"instance" testcase "category" "Test" "counter" "tps" "section" test-name} (sorted-map)
			     {"instance" testcase "category" "Test" "counter" "average" "section"  test-name} (sorted-map)
			     {"instance" testcase "category" "Test" "counter" "max" "section"  test-name} (sorted-map)
			     {"instance" testcase "category" "Test" "counter" "min" "section"  test-name} (sorted-map)
			     {"instance" testcase "category" "Test" "counter" "mean" "section"  test-name} (sorted-map)
			     {"instance" testcase "category" "Test" "counter" "errors" "section"  test-name} (sorted-map)}
			    (per-second-statistics (per-second (sort per-time-stamp (val t)))))]
	    
	     (when connection
	     (doseq [v vs]
	       (.add connection (key v) (val v))) 
	     )
	  )))))))
	 
	 
