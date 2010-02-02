(ns se.sj.monitor.mem 
  (:gen-class)
)

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


(comment
  (def data (ref {}))
  (dosync (alter data add "Örjan" 1 3)
	  (alter data add "Örjan" 4 5)
	  (alter data add "Sture" 2 23)
	  (alter data add "Niklas" 44 5))
  (println @data)
  (println (remove-from-each-while @data #(> 3 %)))
  (dosync (alter data remove-from-each-while #(> 3 %)))
  (println @data)
  (print "Will remove ")
  (println (doall (empty-rows @data)))
  (dosync (doall (map #(alter data dissoc %) (empty-rows @data))))
  (println @data)
  (println (by-name @data "Niklas"))
  (dosync (alter data add "Niklas" {6 76,8 65})
	  (alter data add "Niklas" (by-name @data "Örjan"))
	  (alter data add "Olle" {3 45, 2 54}))

  (println @data)
  (println (key-shifted (by-name @data "Niklas") #(- % 20)))
  (println (class (first (key-shift (by-name @data "Niklas") #(- % 20)))))
)