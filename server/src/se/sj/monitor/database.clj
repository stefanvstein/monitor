(ns se.sj.monitor.database
  (:use se.sj.monitor.db)
  (:use se.sj.monitor.mem))

"A wrapper around live data and persistent store"
(def *live-data* nil)

(defmacro using-live 
  [& form]
  `(binding [*live-data* (ref {})]
     (do ~@form)))

(defmacro using-history
  [path & form]
  `(let [directory# (if (= java.io.File (class ~path))
		      ~path
		      (java.io.File. ~path))]
     (when-not (. directory# isDirectory)
       (. directory# mkdirs))
     (using-db ~path "rawdata" [:date :host :category :counter :instance :jvm] ~@form)))

(defn add-data 
  [name time-stamp value]
  {:pre [(instance? java.util.Date time-stamp)]}
  (when *live-data* (dosync (alter *live-data* add name time-stamp value)))
  (add-to-db value time-stamp name))            ;använd seque

(defn clean-live-data-older-than 
  [timestamp]
  {:pre [(instance? java.util.Date timestamp)]}
  (when *live-data*
    (dosync
     (alter *live-data* remove-from-each-while #(< (.getTime %) (.getTime timestamp)))
     (alter *live-data* empty-names))))

(defn- day-by-day
  [date]
  (let [start (doto (. java.util.Calendar getInstance ) (.setTime date))
	nextfn (fn nextfn [cal] 
		  (lazy-seq 
		   (cons (. cal getTime) 
			 (nextfn (doto cal (.add java.util.Calendar/DATE 1))))))]
    (nextfn start)))

(comment
(using-live (using-history "/home/stefan/testdb2"
			   (add-data {:host "Arne"} (java.util.Date.) 32)
			   (let [df (java.text.SimpleDateFormat. date-format)
				 startDate (. df parse "20100111")
				 endDate (. df parse "20110111")]
			     (names-where (fn [_] true) startDate endDate))))

(using-live (using-history "/home/stefan/testdb2"
			   (add-data {:host "Arne"} (java.util.Date.) 32)
			   (add-data {:host "Olle"} (java.util.Date.) 32)
			   (names-where (fn [_] true))))

(using-live (using-history "/home/stefan/testdb2"
			  
			   (let [df (java.text.SimpleDateFormat. date-format)
				 startDate (. df parse "20100201")
				 endDate (. df parse "20100230")]
			     (data-by {:host "Arne" } startDate endDate))))
)

(defn names-where 
  ([pred]
     (when *live-data*
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
	 (let [day-after-upper (. (doto (. java.util.Calendar getInstance)
				    (.setTime upper)
				    (.add java.util.Calendar/DATE 1)) getTime)
	       day-after-upper-as-text (. df format day-after-upper)
	       day-seq (take-while #(not 
				     (= day-after-upper-as-text (. df format %))) 
				   (day-by-day lower))]
	   (reduce (fn [result-set names-for-a-day]
		     (let [filtered-data (filter pred names-for-a-day)]
		       (if (empty? filtered-data)
			 result-set
			 (apply conj result-set filtered-data))))
		   #{}
		   (map (fn [day]
			  (names-for-day day))
			day-seq)))))))

(defn data-by 
  ([name-spec lower upper]
  {:pre [(= java.util.Date (class lower) (class upper))]}

  (let [indexed-names-and-data #(reduce (fn [b index] 
					 (if (contains? *indices* (key index))  
					   (assoc b (key index) (val index))
					   b))
					   {}
					   name-spec)
	names-and-data-seq (reduce #(conj %1 (key %2) (val %2)) [] (indexed-names-and-data))
	df (java.text.SimpleDateFormat. date-format)
	day-after-upper (. (doto (. java.util.Calendar getInstance)
			     (.setTime upper)
			     (.add java.util.Calendar/DATE 1)) getTime)
	day-after-upper-as-text (. df format day-after-upper)
	day-seq (take-while #(not 
			      (= day-after-upper-as-text (. df format %))) 
			    (day-by-day lower))]
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
	    (map #(conj names-and-data-seq :date (. df format %)) day-seq))))
  ([pred]
     (when *live-data*
       (where-name @*live-data* pred))))

