(ns se.sj.monitor.mem 
  (:use clojure.test)
  (:use clojure.inspector))

;This name space contain functions for delaing with databases represented by a map of sorted maps
;In general the following conventions are used:
;database referes to a map of sorted-map
;name referes to a key of the database
;key referes to key of a sorted map in the database 

(defn added-to
  "Returns sorted-map associated with name in database, 
  with key value added"
  ([database name kvs]
     (let [fun  (fn [d row] (reduce #(assoc %1 (key %2)  (val %2)) row d))]
       (if-let [row (get database name)]
	 (fun kvs row)
	 (let [row (sorted-map)]
	   (fun kvs row)))))  
  ([database name key value]
     (if-let [row (get database name)]
       (assoc row key value)
       (sorted-map key  value))))

(defn add
  "Returns database with key and value added to the sorted-map associated with name. Or, with all entries found in another-database added."
  ([database name key value]
    (assoc database name (added-to database name key value)))
  ([database name kvs]
     (assoc database name (added-to database name kvs)))
  ([database another-database]
     (reduce (fn [db new-data]
	       (add db 
		(first new-data) 
		(first (second new-data)) 
		(second (second new-data)))) 
	     database 
	     (reduce (fn [data a-key] 
		       (apply conj data 
			     (map #(list a-key %)
				  (seq (get another-database a-key)))))
		     [] 
		     (keys another-database)))))


(defn by-name
  "Returns current sorted-map of name in database, nil if non existing. 
  Database is expected to be map, where value is sorted-map."
  [database name]
  (get database name))

(defn where-name
  "Returns a sub-database of database where all names are pred"
  [database pred]
  (let [thekeys (filter pred (keys database))]
    (select-keys database thekeys)))

(defn with-keys-between
  "Returns a sub-database where keys are between lower (inclusive) and upper (exclusive) bounds"
  [database lower upper]
 
  (reduce (fn [newdb kv-from-old] 
	    (assoc newdb (key kv-from-old)
		   (reduce  (fn [new-sorted kv-within] 
			      (assoc new-sorted 
				(first kv-within) 
				(second kv-within))) 
			    (sorted-map)   
			    (subseq (val kv-from-old) >= lower <  upper))))
	  {} 
	  database))

(defn where-name-with-keys-between [database pred lower upper]
  "Combo of where-name and with-keys-between"
  (let [named (where-name database pred)]
    (with-keys-between named lower upper)))

(defn key-shifted
  "Returns sorted-map of entries in mapp, but with keys shifted with unary fn shift" 
 [mapp shift]
  (reduce #(assoc %1 (shift (key %2)) (val %2)) (sorted-map) mapp))

(defn key-shift 
  "Returns lazy seq of map-entries from mapp, where each key is shifted with unary fn shift"
  [mapp shift]
  (map #(first (array-map (shift (key %1)) (val %1))) mapp))

(defn with-keys-shifted
"Returns database with keys shifted with unary fn shift, for those sorted-maps assoiated with name-pred"
 [database name-pred shift]
 (let [subdbmatching (where-name database name-pred)]
       (reduce 
	#(assoc %1 (key %2) (key-shifted (val %2) shift)) 
	database
	subdbmatching)))

(defn remove-from-each-while 
  "Returns a database where all pred keys are removed. Emptied names are not removed."
  [db pred]
  (let [to-remove
	(map
	 (fn [name datat]
	   (list name (take-while pred (keys datat))))
	 (keys db) (vals db))
        indeces-for-each
	(fn [a b]
					; {"nisse" {1 45, 2 54, 4 23}, "pulver" {2 43, 4 65, 7 43}}
					; -> (("nisse" (1 2)) ("pulver" (2)))
	  (assoc a
	    (first b)
	    (reduce #(dissoc %1 %2)
		    (get a (first b))
		    (second b))))]
    (reduce indeces-for-each db to-remove)))

(defn empty-names
  "Returns a lazy of names for those that are associated with empty sorted-maps in database"
  [database]
  (filter #(not (nil? %)) 
	  (map #(when (empty? %1) %2) (vals database) (keys database)))) 

(deftest test-mem

  (let [ data (ref {})]

  (dosync (alter data add "Örjan" 1 3)
	  (alter data add "Örjan" 4 5)
	  (alter data add "Sture" 2 23)
	  (alter data add "Niklas" 44 5))
  (is (= {"Örjan" {4 5, 1 3}
	  "Sture" {2 23}
	  "Niklas" {44 5 }}
	 @data)
      "add data")

  (is (= {"Örjan" {4 5}, "Sture" {}, "Niklas" {44 5}}
	 (remove-from-each-while @data #(> 3 %)))
      "remove-from-each-while")

  (dosync (alter data remove-from-each-while #(> 3 %)))
  (is (= {"Örjan" {4 5}, "Sture" {}, "Niklas" {44 5}} @data) "remove-from-each-while commited")


  (is (= '("Sture") (doall (empty-names @data))) "the empty-names")

  (dosync (doall (map #(alter data dissoc %) (empty-names @data))))
  (is (= {"Örjan" {4 5}, "Niklas" {44 5}} @data) "empty-names commited")
  
  (is (= {44 5} (by-name @data "Niklas")) "data for a user (Niklas)")
  
  (dosync (alter data add "Niklas" {6 76,8 65})
	  (alter data add "Niklas" (by-name @data "Örjan"))
	  (alter data add "Olle" {3 45, 2 54}))  
  (is (= {"Niklas" {4 5, 6 76, 8 65, 44 5}, 
	  "Örjan" {4 5}, 
	  "Olle" { 2 54 3 45}} 
	 @data) 
      "Additional data added")))
(deftest addind-another
  (let [db {"Niklas" {1 2, 2 4, 3 6}
	   "Örjan" {45 0.3, 46 0.4, 47 0.5}}
       another {"Niklas" {2 8, 4 16}
		"Sture" {1 1, 2 2, 3 4}}]
    (is (= {"Niklas" {1 2, 2 8, 3 6, 4 16}
	    "Örjan" {45 0.3, 46 0.4, 47 0.5}
	    "Sture" {1 1, 2 2, 3 4}}
	   (add db another)) "Adding another")))

(deftest test-shifting
  (let [data (ref {"Niklas" {4 5, 6 76, 8 65, 44 5}, 
		   "Örjan" {4 5}, 
		   "Olle" { 2 54 3 45}})]

  (is (= { (- 4 20) 5, (- 6 20) 76, (- 8 20) 65, (- 44 20) 5} 
	 (key-shifted (by-name @data "Niklas") #(- % 20))) 
      "shifted by -20")

  (is (= '([-16 5]  [-14 76] [-12 65] [24 5])  
	 (key-shift (by-name @data "Niklas") #(- % 20))) 
      "key-shift:ed seq of map entries")
  (dosync (alter data with-keys-shifted #(. % startsWith "N") #(+ 1 %)))
  (is (= {"Niklas" {5 5, 7 76, 9 65, 45 5}, 
	   "Örjan" {4 5}, 
	   "Olle" { 2 54 3 45}}
       @data) "with-keys-shifted for those that has names starting with \"N\"")))

(deftest test-name-and-between
  (let [df #(. (java.text.SimpleDateFormat. "yyyyMMdd-HH") parse % )
	data {"Nilkas" (sorted-map (df "20070101-12") 23.3,
				   (df "20070102-12") 23.4,
				   (df "20070103-12") 23.5,
				   (df "20070104-12") 23.6,
				   (df "20070105-12") 23.7),
	      "Arne" (sorted-map (df "20070100-12") 13,
				 (df "20070102-12") 14,
				 (df "20070104-12") 15,
				 (df "20070106-12") 16),
	      "Nils" (sorted-map (df "20070102-12") 5/3,
				 (df "20070104-12") 6/7,
				 (df "20070106-12") 1)}]
    (let [result (where-name-with-keys-between data 
		   #(. % startsWith "N") 
		   (df "20070103-06") 
		   (df "20070105-14"))]
      (is (= #{23.5 23.6 23.7 6/7} 
	     (reduce #(apply conj %1 (vals %2)) 
		     #{} 
		     (vals result)))))))