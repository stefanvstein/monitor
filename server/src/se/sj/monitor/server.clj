(ns se.sj.monitor.server
  (:use (se.sj.monitor mem perfmon) )
  (:use [clojure.contrib.duck-streams :only (reader)] )
  (:use clojure.test)
  (:import (java.io BufferedReader FileReader)))
;This should be private to a perfmon connection. It translates names for a connection
(def perfmon-names (ref {}))

(defn store-name 
  [ names identifier name]
  (dosync (alter perfmon-names
		 (assoc names identifier (name-counter 
					  (dissoc-first-found identifier 
							      (reverse perfmon-columns)) 
					  names
					  perfmon-columns
					  name)))))

(defn get-name
  "Returns the full name" 
  [ names identifier]
  (get @names identifier)
  )

(defn create-obj [line]
  (parse-perfmon-string line 
			(fn [a b] 
			  (store-name perfmon-names a b)
			  (list a b))
			(fn [a b] (list a b))
			(fn [a] (list a))
			(fn [a] (list a))))


(defn add-perfmon-from [database filename]
  (with-open [input (reader filename)]
    (doall
     (map (fn [line] 
	    (create-obj line)) 
		(line-seq input)))
    ))


(defn dissoc-first-found 
"Returns m with the first element found in both m and s removed"
  [m s]
  (loop [s s]
    (if ((first s) m) 
      (dissoc m (first s))
      (when (next s)
	(recur (rest s))))))

(defn name-counter 
  [ input names-db fieldspec name]
  (loop [s {} ids {} fields fieldspec]
    (if-let [field (first fields)]
      (if-let [id (field input)]
	(let [currentid (assoc ids field id)
	      restofFields (rest fields)
	      next-s  (assoc s field (get names-db currentid))]
	  (recur next-s
		 currentid 
		 restofFields))
	(assoc s field name))
      (do (println "null") nil))))

(deftest test-name-counter []
  (let [db
	{{:host 2} "myhost", 
	 {:host 2, :category 4} "mycat", 
	 {:host 2, :category 4, :counter 5} "mycount"}]
    
    (is (= 
	 (name-counter 
	  (dissoc-first-found {:host 2 :category 4 :counter 6} 
			      (reverse perfmon-columns)) 
	  db 
	  perfmon-columns
	  "olle")
	 {:host "myhost" :category "mycat" :counter "olle"}
	 ) 
	"adding a counter"))

  (is (= {:1 1} (name-counter {} {} [:1 ] 1)) "soleley one")
  (is (= nil (name-counter {} {} [] 1)) "is nil")
  (let [db {:1 1}]
    (is (= 
	 {:1 1} 
	 (name-counter 
	  (dissoc-first-found { :1 1 } [:1]) 
	  db
	  [:1]
	  1))
	 "one")))