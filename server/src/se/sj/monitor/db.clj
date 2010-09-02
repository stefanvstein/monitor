(ns se.sj.monitor.db
  (:use cupboard.bdb.je)
  (:use cupboard.utils)
  (:use [clojure stacktrace test])
  (:use [clojure.contrib profile ])
  (:import java.text.SimpleDateFormat)
  (:import java.util.Date)
  (:import [java.nio ByteBuffer])
  (:import [java.io ByteArrayOutputStream DataOutputStream])

  (:import [com.sleepycat.je OperationStatus DatabaseException LockConflictException]))

(def *db* (ref nil))
(def *db-env* (ref nil)) 
(def *next-key* (ref nil))
(def *indices* (ref nil))
(def dayindex-db (atom nil))
; A set of day-structures by date
(def current-days (atom {}))
(def lru (atom []))

(defn host-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 20))


(defn category-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 24))


(defn counter-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 28))


(defn instance-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 32))
   
(defn day-of [bytes]
  (.getInt (ByteBuffer/wrap bytes)))


(def index-keywords-creators (zipmap 
			       [:host :category :counter :instance :day]
			       [host-index-of 
				category-index-of 
				counter-index-of 
				instance-index-of 
				day-of]))

(defn- put-last [s e]
     (lazy-cat (remove #(= e %) s) [e]))

(defn day-as-int 
  ([date]
     (let [par #(Integer/parseInt (.format (SimpleDateFormat. "yyyyMMdd") %))]
       (if date
	 (condp = (class date)
	   Date (par date)
	   String (Integer/parseInt date)
	   (if (instance? Number date)
	     (int date)
	     0))
	 (par (Date.)))))
  ([]
     (day-as-int nil)))

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

(defn name-for-index [day index]
  (get (:by-index (get-day day)) index))

(defn save-index [days day]
  (when @dayindex-db
    (when-let [current (get days day)]
      (db-put @dayindex-db day (:by-index current)))))


(defn index-for-name [day name]
  (let [the-day (get-day day)]
    (if-let [index (get (:by-name the-day) name)]
      index
      (do
	(let [new-days 
	      (swap! current-days 
		     (fn [cd] 
		       (if-let [the-day (get cd day)]
			 (if-let [index (get (:by-name the-day) name)]
			   cd
			   (let [index (:next-index the-day)
				 next-index (inc index)
				 by-name (assoc (:by-name the-day) name index)
				 by-index (assoc (:by-index the-day) index name)]
			     (assoc cd day {:by-name by-name :by-index by-index :next-index next-index})))
			 (assoc cd day {:by-name {name 0} :by-index {0 name} :next-index 1}))))]
	  (save-index new-days day)
	  (get (:by-name (get new-days day)) name))))))



(defn to-bytes [data]
  (let [array-stream (ByteArrayOutputStream.)
	data-out (DataOutputStream. array-stream)
	time-stamp (:time data)
	day (day-as-int time-stamp)
	the-day (get-day day)]
    
    (.writeInt data-out day) ;denna skall bort
    (.writeLong data-out (.getTime #^Date time-stamp))
    (.writeDouble data-out (:value data))
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

(defn- causes [e] 
  (take-while #(not (nil? %)) 
	      (iterate (fn [#^Throwable i] (when i (. i getCause))) 
		       e)))



(defn incremental-key 
"For internal use, but public for use in macro using-db"
  ([db]
     (let [the-next-key 
	   (atom (with-db-cursor [cur db]
		   (let [last-entry (db-cursor-last cur)]
		     (if (empty? last-entry)
		       0
		       (inc (first last-entry))))))]
       #(dec (swap! the-next-key inc)))))


; :cache-bytes (* 1024 1024 10)
(defmacro using-db
  "Defines the database to use, that will be closed after body. Path is directory path to db store, dn-name is the name of the db"
  [path db-name & body]
  `(when-let [db-env# (db-env-open ~path  :allow-create true  :transactional true :txn-no-sync true)]
     (try
	(when-let [db# (db-open db-env# ~db-name :allow-create true)]
	  (try
	   (let [next-key# (incremental-key db#)]
	     (when-let [indices# (reduce (fn [res# data#] 
					   (assoc res# 
					     (key data#)
					     (db-sec-open db-env#
							  db#
							  (name (key data#))
							  :allow-create true
							  :sorted-duplicates true
							  :key-creator-fn (val data#))))
					 {} index-keywords-creators)]
	       (try
		(dosync
		 (ref-set *db-env* db-env#)
		 (ref-set *db* db#)
		 (ref-set *next-key* next-key#)
		 (ref-set *indices* indices#))
		(try
		 (do ~@body)
		 (finally
		  (dosync
		   (ref-set *db-env* nil)
		   (ref-set *db* nil)
		   (ref-set *next-key* nil)
		   (ref-set *indices* nil))))
		
		(finally (dorun (map (fn [v#] (db-sec-close v#)) (vals indices#)))))))
	   (finally  (db-close db#))))
	(finally  (db-env-close db-env#)))))



(defn create-bytes-as-vector [bytes]
  (let [asmap (from-bytes bytes)
	time-stamp (:time asmap)
	value (:value asmap)
	data (dissoc asmap :time :value :date)]
    [data time-stamp value]))

(defn get-from-db 
  [fun & keys-and-data]
  (let [indexes-and-data (apply hash-map keys-and-data)
	is-valid-indexes (filter (complement (set (keys index-keywords-creators)))
				 (keys indexes-and-data))
	day (day-as-int (:day indexes-and-data))
	close-cursor #(db-cursor-close %)
	close-cursors #(dorun (map close-cursor %))
	open-cursor (fn [opened-cursors index-and-data]
		      (let [cursor (db-cursor-open ((key index-and-data) @*indices*) :isolation :read-uncommited)]
			(try
			 (let [index-of-name (if (not (= :day (key index-and-data)))
					       (index-for-name day (val index-and-data))
					       (day-as-int (val index-and-data)))]
			   (if (empty? (db-cursor-search cursor index-of-name :exact true))
			     (do 
			        (close-cursor cursor) 
				 opened-cursors)
			     (conj opened-cursors cursor)))
			 (catch Exception e
			   (close-cursors (conj opened-cursors cursor))
			   (throw e)))))]
    (when is-valid-indexes     
      (let [failed (atom true)
	    retries (atom 0)
	    r (atom nil)]
	(while (and @failed (> 500 @retries))
	       (try
		(let [cursors (reduce open-cursor [] indexes-and-data)]
		  (try
		   (when (and (= (count cursors) 
				 (count indexes-and-data)) 
			      (not (empty? cursors)))
		     (with-db-join-cursor [join cursors]
		       (let [p (fn p [jo]
				 (let [ne (db-join-cursor-next jo)]
				   (if (empty? ne)
				     nil
				     (let [data (create-bytes-as-vector (second ne))]
				       (lazy-seq (cons data (p jo)))))))]
			 (reset! r (fun (p join))))))
		   (finally (close-cursors cursors)))
		  (reset! failed false))
		(catch Exception e
		  (if (some #(instance? LockConflictException %) (causes e))
		    (do (swap! retries (fn [c] (inc c))) 
			(println (str (java.util.Date.) " deadlock in read"))
			(try (Thread/sleep 500) (catch Exception e)))
		    (throw e)))))
	@r))))

(defn add-to-db 
"Adds an entry to db. There will be an index of day-stamps if index :date is currently in current set of inidices. keyword-keys in keys will be indices, if those inidices is currently in use."
 ([db next-key value time keys]
    (when (:time keys)
      (throw (IllegalArgumentException. 
	      (str "Keys may not contain a :time field. :time " 
		   (:time keys)))))
    (when (:date keys)
      (throw (IllegalArgumentException. 
	      (str "Keys may not contain a :date field. :date " 
		   (:date keys)))))
    (when (:value keys)
      (throw (IllegalArgumentException. 
	      (str "Keys may not contain a :value field. :value " 
		   (:value keys))))) 
      
    (let [data (to-bytes (assoc keys 
			   :time time
			   :value value))
	  failed (atom true)
	  retries (atom 0)]
      
      (while (and @failed (> 500 @retries))  
	     (try
	      (db-put db 
		      (.array (.putLong (ByteBuffer/allocate 8) (next-key)))
		      data)
	      (reset! failed false)
	      (catch Exception e 
		(if (some #(instance? LockConflictException %)(causes e))
		  (do (swap! retries (fn [c] (inc c)))
		      (println (str (java.util.Date.) " deadlock in add"))
		      (try (Thread/sleep 500)
			   (catch Exception e)))
		  (throw e)))))))

 ([data time keys]
    (add-to-db @*db* @*next-key* data time keys)))

(comment
(deftest test-db
  (let [tmp (make-temp-dir)
	df (SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (try
     (using-db tmp "test" [:date :olle :nisse]

	       (add-to-db 35.5 (dparse "20070101 120000") {:olle "Arne" :nisse "Nils"})
	       (add-to-db 36.5 (dparse "20070101 120002") {:olle "Arne" :nisse "Nils"})

	       (add-to-db 12 (dparse "20070101 120000") {:olle "Olof" :nisse "Nils"})
	       (add-to-db 13 (dparse "20070101 120001") {:olle "Olof" :nisse "Nils"})
	       (add-to-db 14 (dparse "20070101 120002") {:olle "Olof" :nisse "Nils"})
	       (add-to-db 15 (dparse "20070101 120003") {:olle "Olof" :nisse "Nils"})

	       (add-to-db 2 (dparse "20070101 120001") {:olle "Olof" :nisse "Gustav"})
	       (add-to-db 3 (dparse "20070101 120003") {:olle "Olof" :nisse "Gustav"})
	       (add-to-db 4 (dparse "20070101 120005") {:olle "Olof" :nisse "Gustav"})
	       
;	       (all-in-main #(is (= 9 (count %)) "all is 9"))
	       (all-in-every #(is (= 7 (count %)) "7 Olof") :olle "Olof")
	       (all-in-every #(is (= 3 (count %)) "3 Olof Gustav") :olle "Olof" :nisse "Gustav")
	       (all-in-every #(is (= 6 (count %)) "6 Nils") :nisse "Nils")
	       (add-to-db 36.7 (dparse "20070102 090000") {:olle "Arne" :nisse "Nils"})
	       (all-in-every #(is (= 7 (count %)) "7 Nils") :nisse "Nils")
	       (all-in-every #(is (= 9 (count %)) "9 20070101") :date "20070101")
	       (all-in-every #(is (=  36.7 (nth  (first %) 2))) :date "20070102")
	       (all-in-every #(is (= 2 (count %)) ) :olle "Arne" :date "20070101")
	       (println (str "-----" (all-in-every #(nth (first %) 2) :date "20070102")))
	       (is (= #{35.5 36.5} (all-in-every #(reduce (fn [s v] 
							    (println (str "v:" v))
							     (conj s (nth v 2))) 
							   #{} 
							   %)
			     :date "20070101" :olle "Arne")))
	       (remove-from-db :date "20070102")
;	       (all-in-main #(is (= 9 (count %)) "all is 9"))
 	       (remove-from-db :date "20060102")
;	       (all-in-main #(is (= 9 (count %)) "all is 9"))
	       (remove-from-db :date "20070101")
;	       (all-in-main #(is (= 0 (count %)) "all is 0"))



	       (add-to-db 36.5 (dparse "20070101 120002") {:olle "Arne" :nisse "Nils"})
	       (add-to-db 37.5 (dparse "20070104 120002") {:olle "Arne" :nisse "Nils"})
	       (add-to-db 35.5 (dparse "20070101 120000") {:olle "Arne" :nisse "Nils"})
	       

;	       (remove-until-from-db :date "20070103")

;	         #(is (= 1 (all-in-main (count %)) "all is 1"))
;	       (is (= 37.5 (all-in-main #(nth  (first %) 2))))
	      

	       )
     (finally (rmdir-recursive tmp)))))

)

(deftest test-db
  (let [tmp (make-temp-dir)
	df (SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
   
     (using-db tmp "test"
	       (add-to-db 2 (dparse "20070101 120001") {:category "Olof" :counter "Gustav" :instance "Nisse" :host "Adam"})
	       (println "count" (db-count *db*))
	       
	       (get-from-db #(println "res:" %) :day "20070101" :host "Adam" :category "Olof" :counter "Gustav" )
	       )
   
))