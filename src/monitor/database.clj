(ns monitor.database
  (:use [monitor db mem tools])
  (:use clojure.test)
  (:use clojure.contrib.logging)
;  (:use cupboard.utils)
  (:import (java.util Date Calendar)))

"A wrapper around live data and persistent store"
(def *live-data* (ref nil))

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
	       
(defn sync-db []
  (sync-database))

(defn add-data 
  "Adding data to the store, live if bound, and history if bound. Name is expected to be a map of keyword and string value, possibly used as indices of the data. Time-stamp is expected to be a Date object."
  ([name time-stamp value save?]
     {:pre [(instance? java.util.Date time-stamp)]}
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
	dates-to-remove (filter (fn [adate] (> until adate)) (days-with-data))]
    
    (dorun (map (fn [a-date] 
		  (remove-date a-date))
		dates-to-remove))))

(defn compress-older-than [timestamp, termination-pred]
  {:pre [(instance? java.util.Date timestamp)]}
  
  (let [until (day-as-int timestamp)  
	dates-to-compress (filter (fn [adate] (> until adate)) (days-with-data))]
    (info (str "About to compress data older than " until))
    (doseq [a-date dates-to-compress]
      (when-let [s (compress-data a-date termination-pred)]
	(info s)))))
  
  
(defn clean-live-data-older-than 
  "Remove data in live-data that has timestamp older than timestamp. Removes empty rows"
  [#^Date timestamp]
  {:pre [(instance? java.util.Date timestamp)]}
  (when @*live-data*
    (dosync
     (alter *live-data* remove-from-each-while (fn [#^Date i] (< (.getTime i) (.getTime timestamp))))
     (alter *live-data* (fn [ld] (apply dissoc ld  (empty-names ld)))))))



(defn names-where 
  ([pred]
     (when @*live-data*
       (filter pred (keys @*live-data*))))
  ([#^Date lower #^Date upper]
     {:pre [(instance? java.util.Date lower)
	    (instance? java.util.Date upper)]}
    
       (if (> (. lower getTime) (. upper getTime))
	 (names-where upper lower)
	 (let [res (seq (reduce (fn [result-set names-for-a-day]
				  (if (empty? names-for-a-day)
				    result-set
				    (apply conj result-set names-for-a-day)))
				#{}
				(map (fn [day]
				       (names (day-as-int day)))
				     (day-seq lower upper))))]
	   res))))




(defn data-by-i
([#^Date lower #^Date upper combinations-vectors]
   {:pre [(= java.util.Date (class lower) (class upper))]}
   
  (if (> (. lower getTime) (. upper getTime))
    nil
    (let [lower-long (.getTime lower)
	  upper-long (.getTime upper)
	  ]
      (reduce (fn [r combination]
		(let [name (second combination)
		      day (day-as-int (first combination))
		      fns-for-this-name (if-let [t (get r name)]
					  t
					  [])]
		  (assoc r name (conj fns-for-this-name
				      (fn [a-fn]
					(let [call-if-within (fn [time-values]
							   (doseq [tv (filter
								      (fn [a-tv]
									(let [timestamp-long (.getTime #^java.util.Date (first a-tv))]
									  (and (> timestamp-long lower-long)
									       (< timestamp-long upper-long))))
								      time-values)]
							     (a-fn tv)))]
					  (get-from-db call-if-within
						       day
						       name)))))))
	      {} combinations-vectors))))
  ([pred]
     (when @*live-data*
       (where-name @*live-data* pred)))
)


(defn data-by
  ([pred]
     (data-by-i pred))
  ([#^Date lower #^Date upper & name-spec]
     (let [days (day-seq lower upper)
	   numes (fn [day] (let [t (day-as-int day)
				 data [t (names t)]]
			     data))
	   spec-map (reduce #(assoc %1 
			       (first %2) 
			       (if-let [l ((first %2) %1)] 
				 (conj l (list (first %2) (second %2)))
				 (list (list (first %2) (second %2)))))
			    {} 
			    (partition 2 name-spec))
	   combinations (apply apply-for (vals spec-map))
	   kombinations-maps (reduce (fn [r e]
				      (conj r (reduce (fn [r e]
						(assoc r (first e) (second e))
						) {} e)))
				     []
				     combinations)
	   matching        (reduce (fn [r e]
				     (let [available (numes e)]
				       (let [filtered (filter #(not (nil? %))
					     (for [a (second available) k kombinations-maps]
						 (when (= k (select-keys a (keys k)))
						   [(first available) a])))]
					 (if (seq filtered)
					   (apply conj r filtered)
					   r))))
				   [] days)]
	   (data-by-i lower upper matching))))

(deftest test-unfiltered
   (with-temp-dir
    (let [;tmp (make-temp-dir)
	  df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	  dparse #(. df parse %)
	  as-map (fn [fns-for-names]
			  (let [result (java.util.HashMap.)]
			    (doseq [fns-for-name fns-for-names]
			      (let [name (key fns-for-name)
				    values (java.util.TreeMap.)]
				(.put result name values)
				(doseq [a-fn (second fns-for-name)]
				  (a-fn #(.put values (first %) (second %))))))
			    result))]
		

      (using-history *temp-dir*
		     (add-data {:host "Arne"} (dparse "20100111 100000")  32)
		     (add-data {:host "Arne"} (dparse "20100111 100002")  33)
		     (add-data {:host "Arne"} (dparse "20100111 100003")  34)
		     (add-data {:host "Arne"} (dparse "20100112 100000")  32)
		     (add-data {:host "Arne"} (dparse "20100112 100002")  33)
		     (add-data {:host "Arne"} (dparse "20100112 100003")  34)
		     (add-data {:host "Bertil"} (dparse "20100111 120000")  32)
		     (add-data {:host "Bertil"} (dparse "20100111 120002")  33)
		     (add-data {:host "Bertil"} (dparse "20100111 120003")  34)
		     (let [fns-for-names (data-by
					  (dparse "20100111 000000")
					  (dparse "20100112 230000")
					  :host "Arne" :host "Bertil")]
		       (is (= {{:host "Arne"} {(dparse "20100111 100000")  32
					       (dparse "20100111 100002")  33
					       (dparse "20100111 100003")  34
					       (dparse "20100112 100000")  32
					       (dparse "20100112 100002")  33
					       (dparse "20100112 100003")  34}
			       {:host "Bertil"} {(dparse "20100111 120000")  32
							  (dparse "20100111 120002")  33
							  (dparse "20100111 120003")  34}}
			      (as-map fns-for-names))))))))










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
  (with-temp-dir
  (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (try
     (using-live 
      (using-history *temp-dir*
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
		       (is (nil? (first result))))))))))

      



(deftest test-remove
  (with-temp-dir
  (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]

     (using-live 
      (using-history *temp-dir*
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
)) )))


(deftest test-names-where-many
					;  (println "Running commented")
  (with-temp-dir
  (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
   
     (using-live 
      (using-history *temp-dir*
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
			   "Only the Arne with Silja in live" )))))))
     

(comment
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
)  


