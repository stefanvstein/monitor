(ns monitor.database
  (:use [monitor db mem tools])
  (:use clojure.test)
  (:use clojure.contrib.logging)
  (:use cupboard.utils)
  (:import (java.util Date Calendar)))

"A wrapper around live data and persistent store"
(def *live-data* (ref nil))
;(defonce lock (Object.))

(defmacro using-live
  "A binding around using another live data" 
  [& form]
  `(do 
     (dosync (alter *live-data* (fn [_#] {})))
     (try
      (do ~@form)
      (finally (dosync (alter *live-data* (fn [_#] nil)))))))

(defmacro using-history
  "A binding around using history, persisted data. Path can be String or File object"
  [path & form]
  `(let [directory# (if (= java.io.File (class ~path))
		      ~path
		      (java.io.File. ~path))]
     (when-not (. directory# isDirectory)
       (. directory# mkdirs))
     (using-db-env ~path ~@form))) 
	       


(defn add-data 
  "Adding data to the store, live if bound, and history if bound. Name is expected to be a map of keyword and string value, possibly used as indices of the data. Time-stamp is expected to be a Date object."
  ([name time-stamp value save?]
     {:pre [(instance? java.util.Date time-stamp)]}
     ;(println "Adding" name)
  (when @*live-data* 
    (dosync (alter *live-data* add name time-stamp value)))
  (when save?
    (when @*db-env*
;      (println "<-" name time-stamp value)
      (add-to-db value time-stamp name))))
  ([name time-stamp value]
     (add-data name time-stamp value true)))


(defn clean-stored-data-older-than
  [timestamp]
  {:pre [(instance? java.util.Date timestamp)]}
  (let [until (day-as-int timestamp)  
	dates-to-remove (filter (fn [adate] (> until adate)) (all-dates))]
    (dorun (map (fn [a-date] 
		  (remove-date a-date)) dates-to-remove))))

(defn clean-live-data-older-than 
  "Remove data in live-data that has timestamp older than timestamp. Removes empty rows"
  [#^Date timestamp]
  {:pre [(instance? java.util.Date timestamp)]}
  (when @*live-data*
    (dosync
     (alter *live-data* remove-from-each-while (fn [#^Date i] (< (.getTime i) (.getTime timestamp))))
     (alter *live-data* dissoc empty-names @*live-data*))))



(defn names-where 
  ([pred]
     (when @*live-data*
       (filter pred (keys @*live-data*))))
  ([#^Date lower #^Date upper]
     {:pre [(instance? java.util.Date lower)
	    (instance? java.util.Date upper)]}
     (let [start (System/currentTimeMillis)
	   items (atom 0)
	   ;df (java.text.SimpleDateFormat. date-format)
	   ]
       (if (> (. lower getTime) (. upper getTime))
	 (names-where upper lower)
	 (let [res (seq (reduce (fn [result-set names-for-a-day]
				  (if (empty? names-for-a-day)
				    result-set
				    (apply conj result-set names-for-a-day)))
				#{}
				(map (fn [day]
				       (get-from-dayname (day-as-int day)))
				     (day-seq lower upper))))]
	   res)))))




(defn- data-by-unfiltered
([#^Date lower #^Date upper & name-spec]
   {:pre [(= java.util.Date (class lower) (class upper))]}
  (if (> (. lower getTime) (. upper getTime))
    nil
    (let [spec-map (reduce #(assoc %1 
			      (first %2) 
			      (if-let [l ((first %2) %1)] 
				(conj l (list (first %2) (second %2)))
				(list (list (first %2) (second %2)))))
			   {} 
			   (partition 2 name-spec))
	  lower-long (.getTime lower)
	  upper-long (.getTime upper)
	  combinations (apply apply-for (day-seq lower upper) (vals spec-map)) 
	  combinations-vectors (map (fn [row] 
				      (into [(first row)] 
					    (reduce #(conj %1 (first %2) (second %2)) 
						    [] 
						    (rest row)))) 
				    combinations) ]

     (persistent! (reduce (fn [result names-and-data-for-a-day]
	       (if-let [data-added 
			(apply 
			 get-from-db (fn [data-seq] 
					(reduce (fn [d h]
						  (let [#^Date timestamp (first (next h))
							timestamp-long (.getTime timestamp)]
						    
						    (if (and (> timestamp-long lower-long)
							     (< timestamp-long upper-long))
						      (transient-add d 
							   (first h) 
							   timestamp 
							   (first(nnext h)))
						      d))) 
						result
						data-seq))
			 (day-as-int (first names-and-data-for-a-day))
			 (rest names-and-data-for-a-day))]
		 data-added 
		 result))
	     (transient {}) 
	     combinations-vectors))))))

(defn data-by 
  ([#^Date lower #^Date upper & name-spec]
     {:pre [(= java.util.Date (class lower) (class upper))]}
     
  
  (when-not (nil? @*db-env*)
    (if (> (. lower getTime) (. upper getTime))
      (apply data-by upper lower name-spec)
      (do
	(let [data (reduce (fn [b pair] 
			     (if (contains? #{:host :category :counter :instance} (first pair))  
			       (conj b (first pair) (second pair))
			       b))
			   []
			   (partition 2 name-spec))
	      needs (let [in-need (filter 
				   #(not (some (set %) data)) (partition 2 name-spec))]  
		      (reduce #(if-let [row ((first %2) %1)]
				 (assoc %1 (first %2) (conj row (second %2)))
				 (assoc %1 (first %2) (list (second %2))))
			      {} in-need))]
	  (let [
	      from-db (apply data-by-unfiltered lower upper data)
	      keep? (fn [required-key values a-row-key] 
		      (when-let [value-found (required-key a-row-key)]
			(when (some #(= % value-found) values )
			  a-row-key)))
	      fored (fn [data needs] 
		      (for [data-row data a-need needs] 
			(list data-row a-need)))]
	  (persistent! (reduce (fn [result row-and-need]
		    (let [required-key (key (second row-and-need))
			  required-values (val (second row-and-need))
			  row-key (key (first row-and-need))]
		      (if (not (keep? required-key required-values row-key))
			(dissoc! result row-key)
			result)))
		  (transient from-db) (fored from-db needs)))))))))

  ([pred]
     (when @*live-data*
       (where-name @*live-data* pred))))

(def *comments* (ref (sorted-map)))

(defn add-comment
  ([time-stamp comment]
     (when @*comments*
       (dosync (alter *comments* assoc time-stamp comment)
	       (while (< 100 (count @*comments*))
			 (alter *comments* dissoc (key (first @*comments*))))))
     (info comment))
  ([comment]
     (add-comment (java.util.Date.) comment)))

(defn comments 
  ([] @*comments*)
  ([from to]
     (throw (UnsupportedOperationException. "Currently not implemented"))))


(deftest test-names-where-one

  (let [tmp (make-temp-dir)
	df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (try
     (using-live 
      (using-history tmp
		     (add-data {:host "Arne"} (dparse "20100111 100000")  32)
		     (let [result (names-where 
					       (dparse "20100111 000000") 
					       (dparse "20110111 235959"))]
		       (is (= {:host "Arne"} (first result)) "Finding the sole item")
		       (is (nil? (next result)) "no more items, except the sole one" ))
		     (let [result (names-where (fn [_] true))]
		       (is (= {:host "Arne"} (first result)) "Finding the sole item in in-memory db")
		       (is (nil? (next result)) "no more items in in-mem bd"))
		     (let [result (names-where
					       (dparse "20100112 000000") 
					       (dparse "20110101 000000"))]
		       (is (nil? (first result))) "No items the following days")

		     (let [result (names-where 
					       (dparse "20100111 095958") 
					       (dparse "20100111 100000"))]
		       (is (not (nil? (first result))) "Found the item within a 2 sec span"))
		     (let [result (names-where 
					       (dparse "20100111 100000") 
					       (dparse "20100111 100000"))]
		       (is (not (nil? (first result))) "Found the item with exact timing"))
		     (clean-stored-data-older-than (dparse "20100112 090000"))
		     (let [result (names-where 
				   (dparse "20100111 000000") 
				   (dparse "20110111 235959"))]
		       (is (nil? (first result))))))

      
     (finally  (rmdir-recursive tmp)))))


(deftest test-remove
;(println "Hammarby")
  (let [tmp (make-temp-dir)
	df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (try
     (using-live 
      (using-history tmp
		     (let [baseDate (.getTime (dparse "20100111 100000"))]
		       ;(println (str "Start " (java.util.Date.)))
		       (loop [i 0]
			 (add-data {:host "Arne"} (java.util.Date. (+ i baseDate))  (+ i 32))
			 (if (> 5000 i)
			   (recur (inc i))
;			   (println (str "Added " i))
			   ))
		       ;(println (str "Cleaning " (java.util.Date.)))
		       (clean-stored-data-older-than (dparse "20100112 100000")))
		       ;(println (str "Done " (java.util.Date.)))
)) (finally  (rmdir-recursive tmp)))))


(deftest test-names-where-many
;  (println "Running commented")
  (let [tmp (make-temp-dir)
	df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (try
     (using-live 
      (using-history tmp
		     (add-data {:host "Arne"} (dparse "20100111 100000")  32)
		     (add-data {:host "Bertil"} (dparse "20100111 110000")  33)
		     (add-data {:host "Cesar"} (dparse "20100111 120000")  34) 
		     (let [result (names-where 
				   (dparse "20100111 000000") 
				   (dparse "20110111 235959"))]
		       (is (= #{{:host "Bertil"}{:host "Cesar"}{:host "Arne"}}  
			      (set result)) 
			   "The items" ))
		     (let [result (names-where (fn [_] true))]
		       (is (= #{{:host "Bertil"}{:host "Cesar"}{:host "Arne"}}  
			      (set result)) 
			   "The items in live" ))
		     
		     (add-data {:host "Arne"} (dparse "20100111 090000")  31)
		     
		     (add-data {:host "Arne" :category "Silja"} (dparse "20100111 093000")  31.5)
		     
		     (let [result (names-where (fn [el] 
						 (= (:category el) "Silja")))]
		       (is (= #{{:host "Arne" :category "Silja"}}  (set result)) 
			   "Only the Arne with Silja in live" ))))
     
     (finally  (rmdir-recursive tmp)))))

(deftest test-live-only
  (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (using-live 
     (add-data {:host "Arne"} (dparse "20100111 100000")  32)
     (let [result (names-where (fn [_] true))]
       (is (= #{{:host "Arne"}}  
	      (set result)) 
	   "Single in live" ))
     (let [result (names-where (dparse "20100101 080000") 
			       (dparse "20100131 080000"))]
       (is (nil? result)))
     (add-data {:host "Arne"} (dparse "20100111 110000")  33)
     (add-data {:host "Lennart"} (dparse "20100111 110000")  3)
     
     (let [result (data-by #(= (:host %) "Arne"))]
       (is (= {{:host "Arne"} (sorted-map (dparse "20100111 100000") 32 
					  (dparse "20100111 110000") 33)}
	      result)))
     
     (let [result (data-by (fn [_] true))]
       (is (= {{:host "Arne"} (sorted-map (dparse "20100111 100000") 32 
					  (dparse "20100111 110000") 33)
	       {:host "Lennart"} (sorted-map (dparse "20100111 110000")  3)}
	      result)))
     
     (let [result (data-by (dparse "20100111 080000") (dparse "20100111 140000") :host "Arne")]
       (is (= nil result)))

     (let [result (data-by (fn [_] true))]		
       (is (= 2 (count (get result {:host "Arne"}))) "pre verify")
       (is (= 1 (count (get result {:host "Lennart"}))) "pre verify" ))
     (clean-live-data-older-than  (dparse  "20100111 103000"))
     (let [result (data-by (fn [_] true))]
       (is (= 1 (count (get result {:host "Arne"}))) "with arne removed")
       (is (= 1 (count (get result {:host "Lennart"}))) "with lennart remaining" )))))


(deftest test-db-only
  (binding [*live-data* (ref nil)]
  (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)
	tmp (make-temp-dir)]
    (try
     (using-history tmp
		    (add-data {:host "Arne"} (dparse "20100111 100000")  32)
		    (let [result (names-where (fn [_] true))]
		      (is (= nil result) 
			  "no live names" ))
		    (let [result (names-where 
					      (dparse "20100101 080000") 
					      (dparse "20100131 080000"))]
		      (is (= #{{:host "Arne"}} (set result)) "one element"))

		    (add-data {:host "Arne"} (dparse "20100111 110000")  33)
		    (add-data {:host "Lennart"} (dparse "20100111 110000")  3)

		    (is (nil? (data-by (fn [_] true))) "No data live data")
	

		    (let [result (data-by (dparse "20100111 080000") 
					  (dparse "20100111 140000") 
					  :host "Arne")]
		      (is (= {{:host "Arne"} (sorted-map (dparse "20100111 100000") 32 
							 (dparse "20100111 110000") 33)}
			     result) "Only Arne"))
		    
		    (let [result (data-by (dparse "20100111 080000") 
					  (dparse "20100111 140000") 
					  :host "Arne" 
					  :host "Lennart")]
		      (is (= {{:host "Arne"} (sorted-map (dparse "20100111 100000") 32 
							 (dparse "20100111 110000") 33)
			      {:host "Lennart"} (sorted-map (dparse "20100111 110000")  3)}
			     result)))

		    (add-data {:host "Arne" :category "Nisse" :sladd "Olle"} 
			      (dparse "20100111 110000")  
			      34)
		    (let [result (data-by (dparse "20100111 080000") 
					  (dparse "20100111 140000") 
					  :host "Arne" :category "Nisse")]
		      (is (= 1 (count result)) "Only one with category")
		      (is (= {:host "Arne" :category "Nisse" :sladd "Olle"}  
			     (key (first result))) 
			  "With category Nisse"))

		    (let [result (data-by (dparse "20100111 080000") 
					  (dparse "20100111 140000") 
					  :host "Arne" :host "Lennart" :sladd "Olle")]
		      (is (= 1 (count result)) "Only one with sladd")
		      (is (= {:host "Arne" :category "Nisse" :sladd "Olle"}  (key (first result))))))
     (finally (rmdir-recursive tmp)))
    (try
      
      (using-history tmp
		     (add-data {:host "Arne" :category "Nisse"} (dparse "20010101 080000") 3)
		     (add-data {:host "Arne" :category "Nisse"} (dparse "20010101 080001") 4)
		     (add-data {:host "Arne" :category "Nisse"} (dparse "20010101 080003") 7)
		     (add-data {:host "Arne" :category "Olle"} (dparse "20010101 080300") 30)
		     (add-data {:host "Arne" :category "Olle"} (dparse "20010101 080304") 37)
		     (add-data {:host "Arne" :category "Olle"} (dparse "20010101 080305") 42)
		     (let [result (data-by (dparse "20010101 080305") (dparse "20010101 080305")
					   :host "Arne")]
			   (println "Result" result))
		     )
      
      (finally (rmdir-recursive tmp))))))

(deftest test-nothing-bound
  (binding [*live-data* (ref nil)]
  (is (nil? (add-data {:host "Arne" :category "Nisse" :sladd "Olle"} 
	    (java.util.Date.)  
	    34)))
  (is (nil? (data-by (java.util.Date.) 
	   (java.util.Date.) 
	   :host "Arne" :host "Lennart" :sladd "Olle")))
  (is (nil? (data-by (fn [_] true))))

  (is (nil? (names-where (fn [_] true))))
  (is (nil? (clean-live-data-older-than (java.util.Date.))))))
  


