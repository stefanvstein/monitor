(ns se.sj.monitor.newdata
  (:import [java.nio ByteBuffer])
  (:import [java.io ByteArrayOutputStream DataOutputStream])
  (:import [java.text SimpleDateFormat])
  (:import [java.util Date])
  (:use [clojure.test :only [is deftest]])
  (:use [cupboard.bdb.je :only [db-put db-get db-open db-close]])
  (:use [cupboard.utils :only [make-temp-dir rmdir-recursive]] )
  (:use [se.sj.monitor.db :only [using-db *db-env*]]))

;(set! *warn-on-reflection* true)

(def dayindex-db (atom nil))
; A set of day-structures by date
(def current-days (atom {}))
(def lru (atom []))

(defn put-last [s e]
     (lazy-cat (remove #(= e %) s) [e]))

(defn save-index [days day]
  (when @dayindex-db
    (when-let [current (get days day)]
      (db-put @dayindex-db day (:by-index current)))))

(defn load-day 
  "Returns a day structure from database, or an empty if no db is used, or no data was found for the day"
  [day]
  (let [empty (assoc {} :by-index {} :by-name {} :next-index 0)]
    (if @dayindex-db
      (if-let [by-index (second (db-get @dayindex-db day))]
	(do 
;	  (println "Found day")
	  (let [by-name (reduce #(assoc %1 (val %2) (key %2)) {} by-index)]
	    (assoc {} :by-index by-index :by-name by-name :next-index (count by-index))))
	(do ;(println "Created day") 
	    empty))
      empty)))


  
(defn get-day [day]
  ;(println "Get day")
  (if-let [the-day (get @current-days day)]
    the-day
    (let [the-day (load-day day)]
      ;(println "loaded days")
      	(swap! current-days #(assoc % day the-day))
	(swap! lru (fn [d] (let [r (put-last d day)]
			     (if (< 10 (count r))
			       (next r)
			       r))))
	;remove all not found in lru
	(swap! current-days (fn [c] 
			      (select-keys c @lru)))
	the-day)))


(defmacro using-dayindex-db
  [db-env name & form]
  `(when-let [dbii# (db-open ~db-env ~name :allow-create true)]
     (reset! dayindex-db dbii#)
     (reset! current-days {})
     (try
      (do ~@form)
      (finally
       (reset! current-days  {})
       (reset! dayindex-db nil)
       (db-close dbii#)))))


(defn index-for-name [day name]
  (let [the-day (get-day day)]
    (if-let [index (get (:by-name the-day) name)]
      (do ;(println "Found it")
	  index)
      (do
	(let [new-days 
	      (swap! current-days 
		     (fn [cd] 
		       (if-let [the-day (get cd day)]
			 (if-let [index (get (:by-name the-day) name)]
			   (do ;(println "Found, added just prior to transaction") 
			       cd)
			   (do ;(println "Created new") 
			       (let [index (:next-index the-day)
				     next-index (inc index)
				     by-name (assoc (:by-name the-day) name index)
				     by-index (assoc (:by-index the-day) index name)]
				 (assoc cd day {:by-name by-name :by-index by-index :next-index next-index}))))
			 (do ;(println "This day was removed just prior to the transaction")
			     (assoc cd day {:by-name {name 0} :by-index {0 name} :next-index 1})))))]
	;(println (str "Save " day))
	(save-index new-days day)
	;(println (str "New:" new-days " Name:" name))
	;(let [the-day (get new-days day)]
	 ; (println the-day))
	(get (:by-name (get new-days day)) name))))))
				  

      

(defn name-for-index [day index]
  (get (:by-index (get-day day)) index))

;(def *date-formatter* nil)

;(defn new-date-formatter []
;  (SimpleDateFormat. "yyyyMMdd"))

(defn day-as-int 
  ([date]
     (let [par #(Integer/parseInt (.format (SimpleDateFormat. "yyyyMMdd") %))]
       (if date
	 (par date)
	 (par (Date.)))))
  ([]
     (day-as-int nil)))

    

;  ********************************************HERE******** also, add lru removal
(defn to-bytes [data]
  (let [array-stream (ByteArrayOutputStream.)
	data-out (DataOutputStream. array-stream)
;	date-format  (if *date-formatter*
;		       *date-formatter*
;		       (new-date-formatter))
	time-stamp (:time data)
	day (day-as-int time-stamp)
	the-day (get-day day)]
    
    (.writeInt data-out day) ;denna skall bort
    (.writeLong data-out (.getTime #^Date time-stamp))
    (.writeDouble data-out (:value data))
    ;(println (index-for-name day (:host data)))
    (.writeInt data-out (index-for-name day (:host data)))
    (.writeInt data-out (index-for-name day (:category data)))
    (.writeInt data-out (index-for-name day (:counter data)))
    (.writeInt data-out (index-for-name day (:instance data)))
    (let [others (- (count data) 6)]
      (.writeByte data-out others)
      (when (< 0 others)
	(dorun (map (fn [e] 
		      (.writeInt data-out (index-for-name day (name (key e))))
		      (.writeInt data-out (index-for-name day (val e))))
		    (dissoc data :time :value :host :category :counter :instance)))))
    (.close data-out)
    (.toByteArray array-stream)))

(defn day-of [bytes]
  (.getInt (ByteBuffer/wrap bytes)))

(defn time-stamp-of [bytes]
  (.getLong (ByteBuffer/wrap bytes) 4))

(defn host-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 20))

(defn category-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 24))

(defn counter-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 28))

(defn instance-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 32))

(defn from-bytes [bytes]
  (let [buffer (ByteBuffer/wrap bytes)
	day (.getInt buffer) ;denna skall bort
	the-day (get-day day)
	timestamp (Date. (.getLong buffer))
	value (.getDouble buffer)
	host (name-for-index day (.getInt buffer))
	category (name-for-index day (.getInt buffer))
	counter (name-for-index day (.getInt buffer))
	instance (name-for-index day (.getInt buffer))
	additional (int (.get buffer))
	r (assoc {} 
	    :time timestamp 
	    :value value 
	    :host host 
	    :counter counter 
	    :category category 
	    :instance instance)]
    (if (< 0 additional)
      (loop [i 0 v r]
	(if (< i additional)
	  (recur (inc i) 
		 (assoc v 
		   (keyword (name-for-index day (.getInt buffer))) 
		   (name-for-index day (.getInt buffer))))
	  v))
      r)))


(deftest test-index-for-name
  (let [df (SimpleDateFormat. "yyyyMMdd HHmmss")]
    (reset! lru [])
    (reset! current-days {})

      (let [data (assoc {} :time (.parse df "20071126 033332")  :value 23.0 :host "1" :counter "3" :category "2" :instance "4" :slangbella "454")
	    data2 (assoc {} :time (.parse df "20071126 033332") :value 24.0 :host "two" :counter "four" :category "one" :instance "five")]
	
	(is (= 0 (index-for-name "Tokstolle" (:host data))))
	(is (= 0 (index-for-name "Tokstolle" (:host data))))
	(is (= 1 (index-for-name "Tokstolle" (:category data))))
	(is (= 2 (index-for-name "Tokstolle" (:slangbella data)))))))



(deftest testindices-with-persistence
 (let [tmp (make-temp-dir)]
   (try
    (let [df (SimpleDateFormat. "yyyyMMdd HHmmss")]

	(let [data (assoc {} :time (.parse df "20071126 033332")  :value 23.0 :host "1" :counter "3" :category "2" :instance "4")
	      another-data (assoc {} :time (.parse df "20071125 033332")  :value 2.0 :host "1" :counter "3" :category "2" :instance "4")
	      some-more-data (assoc {} :time (.parse df "20071125 033332") :value 3.0 :host "1" :counter "33" :category "2" :instance "44")
	      even-more-data (assoc {} :time (.parse df "20071125 033332") :value 3.0 :host "1" :counter "33" :category "2" :instance "44" :gurkmeja "meja")]

	  (using-db tmp "test" [:date :olle :nisse]
		    (using-dayindex-db @*db-env* "dayindex-test"
				       (let [bytes (to-bytes data) ] 
					 (is (= 20071126 (day-of bytes)))
					 (is (= data (from-bytes bytes)))
					 (is (= 23.0 (:value (from-bytes bytes))))
					 (let [another-bytes (to-bytes another-data)]
					   (is (= 20071125 (day-of another-bytes)))
					   (is (= 23.0 (:value (from-bytes bytes))))
					   (is (= 2.0 (:value (from-bytes another-bytes))))
					   (is (= 23.0 (:value (from-bytes bytes))))
					   (is (= another-data (from-bytes another-bytes)))
					   (let [some-more-bytes (to-bytes some-more-data)]
					     (is (= 2.0 (:value (from-bytes another-bytes))))
					     (is (= 3.0 (:value (from-bytes some-more-bytes))))
					     (is (= 23.0 (:value (from-bytes bytes))))
					     (is (= 2.0 (:value (from-bytes another-bytes))))
					     (is (= 3.0 (:value (from-bytes some-more-bytes))))
					     (is (= #{0 1 2 3 4 5} 
						    (set (for [x [bytes another-bytes some-more-bytes] 
							       y [host-index-of category-index-of counter-index-of instance-index-of]]
							   (y x)))))
					     (is (= #{0 1 2 3} 
						    (set (for [x [another-bytes bytes] 
							       y [host-index-of category-index-of counter-index-of instance-index-of]]
							   (y x)))))
					     (is (= some-more-data (from-bytes some-more-bytes)))
					     (let [even-more-bytes (to-bytes even-more-data)]
					       (is (= even-more-data (from-bytes even-more-bytes)))))))

				       (is (= 6 (index-for-name 20071125 "gurkmeja")))
				       (reset! current-days {})
				       (reset! lru [])
				       (let [some-more-bytes (to-bytes some-more-data)]
					 (is (= 6 (index-for-name 20071125 "gurkmeja")))
					 (is (every? 
					      (reduce #(conj %1 (%2 some-more-bytes)) 
						      #{} 
						      [host-index-of category-index-of counter-index-of instance-index-of])
					      #{4 5}))
					 (is (= some-more-data (from-bytes some-more-bytes)))
					 (let [bytes (to-bytes data)]
					   (is (= 20071126 (day-of bytes)))
					   (is (= data (from-bytes bytes)))
					   (is (= 23.0 (:value (from-bytes bytes)))))
				       
					 ))))))
    (finally (rmdir-recursive tmp))))


(deftest testindices
  (reset! current-days {})
  (reset! lru [])
  (let [df (SimpleDateFormat. "yyyyMMdd HHmmss")]

      (let [data (assoc {} :time (.parse df "20071126 033332")  :value 23.0 :host "1" :counter "3" :category "2" :instance "4")]
	
	(let [bytes (to-bytes data)]
	  (is (= 20071126 (day-of bytes)))
	  (is (= (.parse df "20071126 033332") (Date. (time-stamp-of bytes))))
	  (is (= 0 (host-index-of bytes)))
	  (is (= 1 (category-index-of bytes)))
	  (is (= 2 (counter-index-of bytes)))
	  (is (= 3 (instance-index-of bytes)))
	  (is (= data (from-bytes bytes)))
	  (is (= (reduce #( conj %1 (name-for-index 20071126 %2)) [] (range 4) )
		 ["1" "2" "3" "4"]))
	  (is (= (reduce #( conj %1 (index-for-name 20071126 %2)) [] ["1" "2" "3" "4"] )
		 (range 4)))
	   ))
      

      (let [data (assoc {} :time (.parse df "20071127 033332")  :value 23.0 :host "1" :counter "3" :category "2" :instance "4" :oljemagnat "magnaten")]
	(let [bytes (to-bytes data)]
	  (is (= data (from-bytes bytes)))
	  (is (= (reduce #( conj %1 (name-for-index 20071127 %2)) [] (range 6) )
		 ["1" "2" "3" "4" "oljemagnat" "magnaten"]))
	  (is (= (reduce #( conj %1 (index-for-name 20071126 %2)) [] ["1" "2" "3" "4" "oljemagnat" "magnaten"] )
		 (range 6)))))))

      
