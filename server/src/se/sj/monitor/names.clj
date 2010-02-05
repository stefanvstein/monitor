(ns se.sj.monitor.names
  (:use clojure.test))

(defn- dissoc-first-found 
"Returns m with the first element found in both m and s removed"
  [m s]
  (loop [s s]
    (if (contains? m (first s)) 
      (dissoc m (first s))
      (when (next s)
	(recur (rest s))))))

(defn- non-matching
  "Returns those in of-interest that arent in has-to-be"
  [of-interest has-to-be]
  (first (reduce (fn [nonmatching element] 
		   (if (not (some (set has-to-be) (list element))) 
		     (conj nonmatching element) 
		     nonmatching)) 
		 #{} 
		 of-interest))
  )

(defn name-for
  [input db spec name]
  (when (> (count (keys input)) (count spec))
    (throw (IllegalArgumentException. (str input " has more keys than spec " spec))))
  (when (contains? db input)
    (throw (IllegalArgumentException. (str input " already existing"))))
  (when-let [non-matching (non-matching (keys input) spec)]
    (throw (IllegalArgumentException. (str non-matching " is not in spec " spec))))

  (if (next input)
    (let [parent-id (dissoc-first-found input (reverse spec))
	  missing-field (nth spec (count (keys parent-id)))
	  parent-name (get db parent-id)]
      (if parent-name
	(assoc parent-name missing-field name)
	(throw (IllegalArgumentException. (str "Parent (" parent-id ") for " input " not found"))) 
	))
    {(key (first input)) name}))


(defn store-name 
  [ names identifier name spec]
  (dosync (alter names assoc identifier (name-for 
					  identifier 
					  @names
					  spec
					  name))))

(defn get-name
  "Returns the full name" 
  [ names identifier]
  (get @names identifier)
  )

(deftest test-store-get-name
  (let [db (ref {})
	spec '(:host :category :counter :instance)]
    (store-name db {:host 0} "host 0" spec)
    (is (= {:host "host 0"} (get-name db {:host 0})) "first thing inserted")
    (is (= nil (get-name db {:host 1})) "Not insterted yet")
    (store-name db {:host 1} "host 1" spec)
    (is (= {:host "host 1"} (get-name db {:host 1})) "second thing inserted") 
    (is (= {:host "host 0"} (get-name db {:host 0})) "first thing inserted, found again")))

(deftest test-dissoc-first-found
  (is (= {} 
	 (dissoc-first-found {:host nil} 
			     (reverse [:host :category :counter]))))
  (is (= {} 
	 (dissoc-first-found {:host 2} 
			     (reverse [:host :category]))))
  (is (= {:host 2} 
	 (dissoc-first-found {:host 2 :category 3} 
			     (reverse [:host :category :counter ]))))
  (is (= {:host 2 :category 3} 
	 (dissoc-first-found {:host 2 :category 3 :counter 4} 
			     (reverse [:host :category :counter])))))

(deftest test-name-counter []
  (let [db
	{{:host 2} {:host "myhost"}, 
	 {:host 2, :category 4} {:host "myhost" :category "mycat"}, 
	 {:host 2, :category 4, :counter 5} {:host "myhost" :category "mycat" :counter "mycount"}}
	spec '(:host :category :counter :instance)]
    (is (=
	 {:host "myhost" :category "mycat" :counter "olle"} 
	 (name-for {:host 2 :category 4 :counter 6} 
		   db 
		   spec 
		   "olle")) 
	"adding a counter")
    (is (= {:host "anotherhost"}
	   (name-for {:host 3} db spec "anotherhost"))
	"adding another host")
    (is (= "hmm"
	   (try 
	    (name-for {:host 2} db spec "renamedhost") 
	    (catch IllegalArgumentException e "hmm")))
	"renaming host")
    (is (= "{:host 2, :category 4} already existing"
	   (try 
	    (name-for {:host 2 :category 4} db spec "renamed") 
	    (catch IllegalArgumentException e (.getMessage e))))
	"renaming host")
    (is (= ":a is not in spec (:host :category :counter :instance)"
	   (try 
	    (name-for {:a 2} db spec "should not happen") 
	    (catch IllegalArgumentException e  (. e getMessage) )))
	"illegal :a"))

  (is (= {:1 2} 
	 (name-for {:1 1} {} [:1 ] 2)) 
      "soleley one")
  (is (= {:1 1} 
	 (name-for {:1 1 } {:1 1} [:1] 1))
      "one"))