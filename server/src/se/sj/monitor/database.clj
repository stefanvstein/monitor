(ns se.sj.monitor.database
  (:use [se.sj.monitor db mem])
  (:use clojure.test)
  (:use clojure.contrib.logging)
  (:use cupboard.utils))

"A wrapper around live data and persistent store"
(def *live-data* (ref {}))

(defmacro using-live
  "A binding around using another live data" 
  [& form]
  `(binding [*live-data* (ref {})]
     (do ~@form)))

(defmacro using-history
  "A binding around using history, persisted data. Path can be String or File object"
  [path & form]
  `(let [directory# (if (= java.io.File (class ~path))
		      ~path
		      (java.io.File. ~path))]
     (when-not (. directory# isDirectory)
       (. directory# mkdirs))
     (using-db ~path "rawdata" [:date :host :category :counter :instance :jvm] ~@form)))

(defn add-data 
  "Adding data to the store, live if bound, and history if bound. Name is expected to be a map of keyword and string value, possibly used as indices of the data. Time-stamp is expected to be a Date object."
  [name time-stamp value]
  {:pre [(instance? java.util.Date time-stamp)]}
  (when @*live-data* 
    (dosync (alter *live-data* add name time-stamp value)))
  (when @*db* 
    (add-to-db value time-stamp name)))            ;använd seque

(defn clean-live-data-older-than 
  "Remove data in live-data that has timestamp older than timestamp. Removes empty rows"
  [timestamp]
  {:pre [(instance? java.util.Date timestamp)]}
  (when @*live-data*
    (dosync
     (alter *live-data* remove-from-each-while #(< (.getTime %) (.getTime timestamp)))
     (alter *live-data* dissoc empty-names @*live-data*))))

(defn- day-by-day
  [date]
  (let [start (doto (. java.util.Calendar getInstance ) (.setTime date))
	nextfn (fn nextfn [cal] 
		  (lazy-seq 
		   (cons (. cal getTime) 
			 (nextfn (doto cal (.add java.util.Calendar/DATE 1))))))]
    (nextfn start)))

(defn- day-seq [lower upper]
  (let [df (java.text.SimpleDateFormat. date-format)
	day-after-upper (. (doto (. java.util.Calendar getInstance)
			     (.setTime upper)
			     (.add java.util.Calendar/DATE 1)) getTime)
	day-after-upper-as-text (. df format day-after-upper)]
    (take-while #(not 
		  (= day-after-upper-as-text (. df format %))) 
		(day-by-day lower))))

(defn names-where 
  ([pred]
     (when @*live-data*
       (filter pred (keys @*live-data*))))
  ([pred lower upper]
     {:pre [(instance? java.util.Date lower)
	    (instance? java.util.Date upper)]}
     (let [df (java.text.SimpleDateFormat. date-format)
	   within-dates #( and (<= (.getTime lower) (.getTime (second %)))
			       (>= (.getTime upper) (.getTime (second %))))
	   names-for-day #(all-in-every (fn [seq-of-data] 
					  (reduce (fn [a-set a-result] 
						    (conj a-set (first a-result))) 
						  #{} 
						  (filter within-dates seq-of-data))) 
					:date (. df format %))]
       
       (if (> (. lower getTime) (. upper getTime))
	 nil
	 (seq (reduce (fn [result-set names-for-a-day]
		   (let [filtered-data (filter pred names-for-a-day)]
		     (if (empty? filtered-data)
		       result-set
		       (apply conj result-set filtered-data))))
		 #{}
		 (map (fn [day]
			(names-for-day day))
		      (day-seq lower upper))))))))



(defn- apply-for
  ([a]
     (for [at a] (list at)))
  ([a & more]
     (loop [first-time true
	    results a 
	    coming (first more)
	    remaining (next more)]
       (let [fored (for [r results c coming] 
		     (if first-time 
		       (list r c)
		       (concat r (list c))))]
	 (if remaining
	   (recur false fored (first remaining) (next remaining))
	   fored)))))


(defn- data-by-unfiltered
([lower upper & name-spec]
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
	  df (java.text.SimpleDateFormat. date-format)
	  combinations (apply apply-for (map #(list :date (. df format %)) 
					     (day-seq lower upper)) 
			      (vals spec-map)) 
	  combinations-vectors (map (fn [row] 
				      (reduce #(conj %1 (first %2) (second %2)) 
					      [] 
					      row)) 
				    combinations)]

     (reduce (fn [result names-and-data-for-a-day]
	       (if-let [data-added 
			(apply 
			 all-in-every (fn [data-seq] 
					(reduce #(add %1 
						      (first %2) 
						      (first(next %2)) 
						      (first(nnext %2))) 
						result
						data-seq))
			 names-and-data-for-a-day)]
		 data-added 
		 result))
	     {} 
	     combinations-vectors)
     
    
     ))))

(defn data-by 
  ([lower upper & name-spec]
  {:pre [(= java.util.Date (class lower) (class upper))]}
  (when-not (nil? @*db*)
    (if (> (. lower getTime) (. upper getTime))
      nil
      (do
	(let [data (reduce (fn [b pair] 
			     (if (contains? *indices* (first pair))  
			       (conj b (first pair) (second pair))
			       b))
			   []
			   (partition 2 name-spec))
	      needs (let [in-need (filter 
				   #(not (some (set %) data)) (partition 2 name-spec))]  
		      (reduce #(if-let [row ((first %2) %1)]
				 (assoc %1 (first %2) (conj row (second %2)))
				 (assoc %1 (first %2) (list (second %2))))
			      {} in-need))
	      from-db (apply data-by-unfiltered lower upper data)
	      keep? (fn [required-key values a-row-key] 
		      (when-let [value-found (required-key a-row-key)]
			(when (some #(= % value-found) values )
			  a-row-key)))
	      fored (fn [data needs] 
		      (for [data-row data a-need needs] 
			(list data-row a-need)))]
	  (reduce (fn [result row-and-need]
		    (let [required-key (key (second row-and-need))
			  required-values (val (second row-and-need))
			  row-key (key (first row-and-need))]
		      (if (not (keep? required-key required-values row-key))
			(dissoc result row-key)
			result)))
		  from-db (fored from-db needs)))))))

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
		     (let [result (names-where (fn [_] true) 
					       (dparse "20100111 000000") 
					       (dparse "20110111 235959"))]
		       (is (= {:host "Arne"} (first result)) "Finding the sole item")
		       (is (nil? (next result)) "no more items, except the sole one" ))
		     (let [result (names-where (fn [_] true))]
		       (is (= {:host "Arne"} (first result)) "Finding the sole item in in-memory db")
		       (is (nil? (next result)) "no more items in in-mem bd"))
		     (let [result (names-where (fn [_] true) 
					       (dparse "20100112 000000") 
					       (dparse "20110101 000000"))]
		       (is (nil? (first result))) "No items the following days")
		     (let [result (names-where (fn [_] true) 
					       (dparse "20100111 100001") 
					       (dparse "20110101 000000"))]
		       (is (nil? (first result)) "No more items after the second where the sole is"))
		     (let [result (names-where (fn [_] true) 
					       (dparse "20100111 095958") 
					       (dparse "20100111 095959"))]
		       (is (nil? (first result)) "No items the second before the sole item"))
		     (let [result (names-where (fn [_] true) 
					       (dparse "20100111 095958") 
					       (dparse "20100111 100000"))]
		       (is (not (nil? (first result))) "Found the item within a 2 sec span"))
		     (let [result (names-where (fn [_] true) 
					       (dparse "20100111 100000") 
					       (dparse "20100111 100000"))]
		       (is (not (nil? (first result))) "Found the item with exact timing"))
		     (let [dmilliparse #(. (java.text.SimpleDateFormat. "yyyyMMdd HHmmss.SSS") parse %)
			   result (names-where (fn [_] true) 
					       (dmilliparse "20100111 100000.001") 
					       (dmilliparse "20100111 100000.999"))]
		       (is (nil? (first result)) "No item a few millis to late"))))
     (finally  (rmdir-recursive tmp)))))

(deftest test-names-where-many
  (let [tmp (make-temp-dir)
	df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (try
     (using-live 
      (using-history tmp
		     (add-data {:host "Arne"} (dparse "20100111 100000")  32)
		     (add-data {:host "Bertil"} (dparse "20100111 110000")  33)
		     (add-data {:host "Cesar"} (dparse "20100111 120000")  34) 
		     (let [result (names-where (fn [_] true) 
					       (dparse "20100111 000000") 
					       (dparse "20110111 235959"))]
		       (is (= #{{:host "Bertil"}{:host "Cesar"}{:host "Arne"}}  
			      (set result)) 
			   "The items" ))
		     (let [result (names-where (fn [_] true))]
		       (is (= #{{:host "Bertil"}{:host "Cesar"}{:host "Arne"}}  
			      (set result)) 
			   "The items in live" ))
		     (let [result (names-where (fn [_] true) 
					       (dparse "20100111 113000") 
					       (dparse "20110111 235959"))]
		       (is (= #{{:host "Cesar"}}  (set result)) "The last only" ))
		     (add-data {:host "Arne"} (dparse "20100111 090000")  31)
		     (let [result (names-where (fn [_] true) 
					       (dparse "20100111 080000") 
					       (dparse "20100111 103000"))]
		       (is (= #{{:host "Arne"}}  (set result)) "Only one Arne, while 2 records" ))

		     (add-data {:host "Arne" :category "Silja"} (dparse "20100111 093000")  31.5)
		     (let [result (names-where (fn [_] true) 
					       (dparse "20100111 080000") 
					       (dparse "20100111 103000"))]
		       (is (= #{{:host "Arne"} {:host "Arne" :category "Silja"}}  (set result)) 
			   "Two Arne, one with Silja" ))
		     (let [result (names-where (fn [el] 
						 (= (:category el) "Silja")) 
					       (dparse "20100111 080000") 
					       (dparse "20100111 103000"))]
		       (is (= #{{:host "Arne" :category "Silja"}}  (set result)) 
			   "Only the Arne with Silja" ))
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
     (let [result (names-where (fn [_] true) 
			       (dparse "20100101 080000") 
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
		    (let [result (names-where (fn [_] true) 
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
  (is (nil? (names-where (fn [_] true) 
	       (java.util.Date.) 
	       (java.util.Date.))))
  (is (nil? (names-where (fn [_] true))))
  (is (nil? (clean-live-data-older-than (java.util.Date.))))))
  


