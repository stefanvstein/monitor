(ns se.sj.monitor.mem 
  (:use clojure.test))

;This name space contain functions for delaing with databases represented by a map of sorted maps
;In general the following conventions are used:
;database referes to a map of sorted-map
;name referes to a key of the database
;key referes to key of a sorted map in the database 

(defn added-to
  "Returns sorted-map associated with name in database, 
  with key value added"
  ([database name kvs]
     (let [fun  (fn [d row] (reduce #(assoc %1 (key %2) (val %2)) row d))]
       (if-let [row (get database name)]
	 (fun kvs row)
	 (let [row (sorted-map)]
	   (fun kvs row)))))  
  ([database name key value]
     (if-let [row (get database name)]
       (assoc row key value)
       (sorted-map key value))))

(defn add
  "Returns database with key and value added to the sorted-map associated with name"
  ([database name key value]
     (assoc database name (added-to database name key value)))
  ([database name kvs]
     (assoc database name (added-to database name kvs))))


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