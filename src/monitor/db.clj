(ns monitor.db
  (:use cupboard.bdb.je)
  (:use cupboard.utils)
  (:use [clojure stacktrace test])
  (:use monitor.tools)
  (:import java.text.SimpleDateFormat)
  (:import java.util.Date)
  (:import [java.nio ByteBuffer])
  (:import [java.io ByteArrayOutputStream DataOutputStream])
  (:import [com.sleepycat.je OperationStatus DatabaseException LockConflictException]))



(def *db-env* (ref nil)) 
(def dayindex-db (atom nil)) ;borde vara ref eller var
; A set of day-structures by date
(def current-day-indexes (atom {})) ;borde vara ref
(def lru-days-for-indices (atom [])) ;borde vara ref
(def *day* nil) 
(def *db-struct* nil)

(def opened-data (ref {})) ;namn ref
(def unused-data-queue (ref '())) ;name last is oldest
(def users-per-data (ref {})) ;namn , num of users
(def keep-open-data (ref #{})) ;namn 
(def datas-to-keep-alive 10)

(def opened-wdata (ref {}))
(def unused-wdata-queue (ref '()))
(def wdata-to-keep-alive 2)
  
;(def dayname-db (atom nil))
;(def dayname-cache (atom nil))
;(def all-days (atom #{}))

(def lockobj (Object.))

(defn sync-database []
  (db-env-sync *db-env*))

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

(defn days-with-data []
  (map #(Integer/parseInt %) (filter #(re-matches #"\d{8}" %) (seq (db-env-db-names *db-env*)))))

;---Start of day index name stuff
(defn- load-day 
  "Returns a day structure from database, or an empty if no db is used, or no data was found for the day"
  [day]
  (let [empty (assoc {} :by-index {} :by-name {} :next-index 0)]
    (if @dayindex-db
      (if-let [by-index (second (db-get @dayindex-db day))]
	(let [by-name (reduce #(assoc %1 (val %2) (key %2)) {} by-index)]
	  (assoc {} :by-index by-index :by-name by-name :next-index (count by-index)))
	empty)
      empty)))

(defn- remove-day-indices [day]
  (db-delete @dayindex-db day)
  (dosync
   (swap! current-day-indexes #(dissoc % day))
   (swap! lru-days-for-indices (fn [lru] (vec (filter #(not= day %) lru))))
   )
  )
  
(defn- get-day [day]
  (if-let [the-day (get @current-day-indexes day)]
    the-day
    (let [the-day (load-day day)]
      	(swap! current-day-indexes #(assoc % day the-day))
	(swap! lru-days-for-indices (fn [d] (let [r (put-last d day)]
			     (if (< 10 (count r))
			       (next r)
			       r))))
	(swap! current-day-indexes (fn [c] 
			      (select-keys c @lru-days-for-indices)))
	the-day)))

(defn names [day]
  (keys (:by-name (get-day day))))

(defn- name-for-index [day index]
  (get (:by-index (get-day day)) index))

(defn- save-index [days day]
  (when @dayindex-db
    (when-let [current (get days day)]
      (let [failed (atom true)
	    retries (atom 0)]
	(while (and @failed (> 500 @retries))
	  (try
	    (db-put @dayindex-db day (:by-index current))
	    (reset! failed false)
	    (catch Exception e
	      (if (some #(instance? LockConflictException %) (causes e))
		(do (swap! retries (fn [c] (inc c)))
		    (println (java.util.Date.) " deadlock in save-index")
		    (try (Thread/sleep (rand-int 100))
			 (catch Exception e)))
		(throw e)))))))))

(defn- index-for-name [day name]
  (let [the-day (get-day day)]
    (if-let [index (get (:by-name the-day) name)]
      index
      (do
	(let [new-days 
	      (swap! current-day-indexes 
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



(defn- integral?
  "returns true if a number is actually an integer (that is, has no fractional part)"
  [x]
  (cond
   (integer? x) true
   (decimal? x) (>= (.ulp (.stripTrailingZeros (bigdec 0))) 1) ; true iff no fractional part
   (float? x)   (= x (Math/floor x))
   (ratio? x)   (let [^clojure.lang.Ratio r x]
                  (= 0 (rem (.numerator r) (.denominator r))))
   :else        false))

(defn- to-bytes
  ([value time-stamp]
  (let [array-stream (ByteArrayOutputStream.)
	data-out (DataOutputStream. array-stream)]

    (.writeInt data-out 1)
    (.writeLong data-out (.getTime #^Date time-stamp))
    (if (integral? value)
      (do (.writeByte data-out 1)
	  (.writeLong data-out (long value)))
      (do (.writeByte data-out 2)
	  (.writeDouble data-out (double value))))
    (.close data-out)
    (.toByteArray array-stream)))

  ([time-and-values]
     (let [array-stream (ByteArrayOutputStream.)
	   data-out (DataOutputStream. array-stream)]
       (.writeInt data-out (count time-and-values))
       (doseq [e time-and-values]
	 (.writeLong data-out (.getTime #^Date (key e)))
	 (let [value (val e)]
	   (if (integral? value)
	     (do (.writeByte data-out 1)
		 (.writeLong data-out (long value)))
	     (do (.writeByte data-out 2)
		 (.writeDouble data-out (double value))))
	   ))
       (.close data-out)
       (.toByteArray array-stream)
       )))


(defn from-bytes [bytes]
  (let [buffer (ByteBuffer/wrap bytes)
	i (.getInt buffer)
	p (fn p [buffer i]
	    (when-not (= 0 i)
	      (lazy-seq (cons (let [timestamp (Date. (.getLong #^ByteBuffer buffer))
				    value-type (int (.get #^ByteBuffer buffer))
				    value (if (= 1 value-type)
					    (.getLong #^ByteBuffer buffer)
					    (.getDouble #^ByteBuffer buffer))]
				[timestamp value])
			      (p buffer (dec i))))))]
    (p buffer i)))
    


    





(defn- incremental-key 
"For internal use, but public for use in macro using-db"
  ([db]
     (let [the-next-key 
	   (atom (with-db-cursor [cur db]
		   (let [last-entry (db-cursor-last cur)]
		     (if (empty? last-entry)
		       0
		       (inc (.getLong (ByteBuffer/wrap (first last-entry))))))))]
       #(dec (swap! the-next-key inc)))))


; :cache-bytes (* 1024 1024 10)
(defmacro using-db-env [path & body]
  `(when-let [db-env# (db-env-open ~path  :allow-create true  :transactional true :txn-no-sync true :cache-bytes (* 500 1024 1024))]
     (when-let [dayindex-db# (db-open db-env# "day-index" :allow-create true)]
       (reset! dayindex-db dayindex-db#)
       (reset! current-day-indexes {})
       (reset! lru-days-for-indices [])
       (try
	(dosync (ref-set *db-env* db-env#))
	(try
	 (try
	  (do ~@body)
	  (finally
	   (dorun (map (fn [g#] (println "Database" g# "is closed by shutdown of environment")) (keys @users-per-data)))
	   (dorun (map (fn [h#] 
			 (close-db h#)) 
		       (vals @opened-data)))
	   (dorun (map (fn [h#] 
			 (close-db h#)) 
		       (vals @opened-wdata)))
	   (reset! current-day-indexes  {})
	   (reset! dayindex-db nil)
	   (reset! lru-days-for-indices [])
					;(evict-dayname-cache)***********************
	   (db-close dayindex-db#)
	   (dosync
	    (ref-set opened-data {})
	    (ref-set unused-data-queue '())
	    (ref-set users-per-data {})
	    (ref-set keep-open-data #{})
	    (ref-set opened-wdata {})
	    (ref-set unused-wdata-queue '())
	  )))
       (finally
	(dosync (ref-set *db-env* nil))))
       
      (finally 
       (db-env-close db-env#))))))



(defn- open-db [db-name allow-create]
  (try
    (when-let [db (db-open @*db-env* (str db-name) :allow-create allow-create :sorted-duplicates true)]
      {:db db, :name db-name})
    (catch com.sleepycat.je.DatabaseNotFoundException e
      (when allow-create
	(throw e)))))

(defn close-db [db]
  (db-close (:db db)))


(defn- clean-after-release
  ([names-to-close]
     (locking lockobj
       (let [should-close (atom '())]
	 (dosync
	  (when-let [to-close (let [to-close (nthnext @unused-data-queue (- datas-to-keep-alive (count @keep-open-data)))]
				(if (seq names-to-close)
				  (apply conj to-close names-to-close)
				  to-close))]
	    (dorun (map (fn [i] 
			  (swap! should-close #(if-let [h (get @opened-data i)]
						 (conj % h)
						 %)))
			to-close))
	    (alter opened-data #(apply dissoc % to-close))
	    (alter unused-data-queue #(apply list (take (- datas-to-keep-alive (count @keep-open-data)) %))))
	  (when-let [to-close (let [to-close (nthnext @unused-wdata-queue (- datas-to-keep-alive (count @keep-open-data)))]
				(if (seq names-to-close)
				  (apply conj to-close names-to-close)
				  to-close))]
	    (dorun (map (fn [i] 
			  (swap! should-close #(if-let [h (get @opened-wdata i)]
						 (conj % h)
						 %)))
			  to-close))
	    (alter opened-wdata #(apply dissoc % to-close))
	    (alter unused-wdata-queue #(apply list (take (- datas-to-keep-alive (count @keep-open-data)) %)))
	    ))	 (dorun (map #(close-db %) @should-close)))))
  ([]
     (clean-after-release '())))
  

(defn release-db [db-name]
  (locking lockobj
    (let [db-name (cond 
		 (= String (class db-name)) (day-as-int db-name)
		 (instance? Number db-name) (day-as-int db-name)
		 (and (associative? db-name) (:name db-name)) (day-as-int (:name db-name))
		 :else (throw (IllegalArgumentException.)))]
		  

    (dosync
     (alter users-per-data assoc db-name (dec (get @users-per-data db-name 1)))
     (when (= 0 (get @users-per-data db-name))
       (when (not (some #{db-name} @keep-open-data))
	 (alter unused-data-queue conj db-name)
      	 (alter unused-wdata-queue conj db-name))
       (alter users-per-data dissoc db-name))
     (clean-after-release)))))


(defn aquire-db 
  ([day create]
     (locking lockobj
       (let [day (day-as-int day)
	     there-are-no #(or (nil? %) (= 0 %))
	     do-aquire-db (fn [day create]
			    (if create
			      (dosync
			       (let [db (get @opened-wdata day)]
				 (alter unused-wdata-queue (fn [c] (remove #(= day %) c)))
				 (alter users-per-data assoc day (inc (get @users-per-data day 0)))
				 db))
			      (dosync
			       (let [db (get @opened-data day)]
				 (alter unused-data-queue (fn [c] (remove #(= day %) c)))
				 (alter users-per-data assoc day (inc (get @users-per-data day 0)))
				 db))))
	     create-db (fn create-db [day create]
			 (if-let [a-db (open-db day create)]
			   (dosync
			    (if create
			      (alter opened-wdata assoc day a-db)
			      (alter opened-data assoc day a-db))
			    (do-aquire-db day create))))
	     upgrade (fn upgrade [day]
		       (release-db day)
		       (clean-after-release [day])
		       (create-db day true))]
	 (if create
	   (if-not (get @opened-wdata day)
	     (if (get @opened-data day)
	       (if (there-are-no (get @users-per-data day))
		 (do (println "Upgradeing" day "to writeable")
		     (upgrade day)) 
		 (println "Sorry!" day "is in use, while being readonly"))
	       (create-db day true))
	     (do-aquire-db day true))
	   (if-not (get @opened-data day)
	     (if-not (get @opened-wdata day)
	       (create-db day false)
	       (do-aquire-db day true))
	     (do-aquire-db day false))))))
  ([day]
     (aquire-db day true)))
  
      


    
       
(defn mark-as-always-opened [db-name]
  (let [db-name (cond 
		 (= String (class db-name)) (day-as-int db-name)
		 (instance? Number db-name) (day-as-int db-name)
		 (and (associative? db-name) (:name db-name)) (day-as-int (:name db-name))
		 :else (throw (IllegalArgumentException.)))]
     (dosync (alter keep-open-data conj db-name))))

(defn mark-as-not-always [db-name]
  (let [db-name (cond 
		 (= String (class db-name)) (day-as-int db-name)
		 (instance? Number db-name) (day-as-int db-name)
		 (and (associative? db-name) (:name db-name)) (day-as-int (:name db-name))
		 :else (throw (IllegalArgumentException.)))]
	(dosync (alter keep-open-data disj db-name)
		(when (= 0 (get @users-per-data db-name 0))
		  (alter users-per-data dissoc db-name)
		  (alter unused-data-queue conj db-name ))
		  (clean-after-release)))) 

(defmacro always-opened [db-name & body]
  `(do 
     (mark-as-always-opened ~db-name)
     (try
      (do ~@body)
      (finally (mark-as-not-always ~db-name)))))

(defmacro using-day [day create & body]
  `(when-let [day# (day-as-int ~day)]
     (if-let [db# (aquire-db day# ~create)]
       (binding [*day* day#
		 *db-struct* db#]  
	 (try
	   (do ~@body)
	   (finally
	    (release-db day#)
	    ))))))


(defn records [date]
  (using-day date false
	     (when (:db *db-struct*)
	       (db-count (:db *db-struct*)))))

(defn remove-date [date]
  (locking lockobj
    (let [date (day-as-int date)
	  there-are-no #(or (nil? %) (= 0 %))]
      (when (not (get @keep-open-data date))
	(dosync
	 (let [active-users (get @users-per-data date)]
	   
	   (when (there-are-no active-users)
					;(swap! all-days disj date)
					;	   (evict-dayname-cache)****************************************
					;---
	     (when-let [db (get @opened-data date)]
	       (try
		 (close-db db)
		 (catch Exception e
		   (println "Ignoring exception while closing" date)
		   (print-stack-trace e))))
	     
	     (alter opened-data dissoc date)
	     (alter unused-data-queue (fn [q] (apply list (remove #(= date %) q))))
	     (when-let [db (get @opened-wdata date)]
	       (try
		 (close-db db)
		 (catch Exception e
		   (println "Ignoring exception while closing" date)
		   (print-stack-trace e))))
	     
	     (alter opened-wdata dissoc date)
	     (alter unused-wdata-queue (fn [q] (apply list (remove #(= date %) q))))
					;---
	     (alter users-per-data dissoc date)
	     (try
	       
	       (let [pattern (re-pattern (str date ".*"))]
		 (dorun (map #(db-env-remove-db *db-env* %) 
			     (filter #(re-matches pattern %) 
				     (seq (db-env-db-names *db-env*))))))
	       (catch Exception e
		 (println "Ignoring exception while deleteing" date)
		 (print-stack-trace e)))
	     (try
					;	    (db-delete @dayname-db date)
	       (remove-day-indices date)
	       
	       (catch Exception e
		 (println "Ignoring exception while deleteing metadata for" date)
		 (print-stack-trace e))))))))))
	      



     ;note that keys and data is a map now
  (defn get-from-db [fun day keys-and-data]
    (let [day (day-as-int day)]
      (when-let [index (index-for-name day keys-and-data)]
	(using-day day false
		   (let [cur (db-cursor-open (:db *db-struct*))]
		     (try
		       (let [first-value (db-cursor-search cur index)
			     p (fn p [cursor data]
				 (if data
				   (if-let [following (next data)]
				     (lazy-seq (cons (first data) (p cursor following)))
				     (lazy-seq (cons (first data) (p cursor nil))))
				   (let [ne (db-cursor-next cursor)]
				     (when (= (first ne) index)
				       (if-let [data (seq (from-bytes (second ne)))]
					 (lazy-seq (cons (first data) (p cursor (next data))))
					 (p cursor nil))))))]
			 (when (= (first first-value) index)
			   (when-let [u (seq (from-bytes (second first-value)))]
			     (fun (lazy-seq (cons (first u) (p cur (next u)))))
			     )))
		       (finally (db-cursor-close cur))))))))

  
(defn add-to-db 
  "Adds an entry to db. There will be an index of day-stamps if index :date is currently in current set of inidices. keyword-keys in keys will be indices, if those inidices is currently in use."
  [value time keys]
  (let [day (day-as-int time)
	index (index-for-name day keys)]
  (using-day (day-as-int time) true
	     (let [data (to-bytes value time)
		   failed (atom true)
		   retries (atom 0)]
	       (while (and @failed (> 500 @retries))  
		      (try
		       (db-put (:db *db-struct*) 
			       index
			       data)
		       (reset! failed false)
		       (catch Exception e 
			 (if (some #(instance? LockConflictException %)(causes e))
			   (do (swap! retries (fn [c] (inc c)))
			       (println (str (java.util.Date.) " deadlock in add"))
			       (try (Thread/sleep (rand-int 100))
				    (catch Exception e)))
			   (throw e)))))))))


(defn compress-data
  ([date termination-fn]
     (let [cname "beingcompressed"
	   remove-db-and-indices (fn [name]
				   (let [pattern (re-pattern (str name ".*"))
					 elements (filter #(re-matches pattern %)
							  (seq (db-env-db-names *db-env*)))]
				     (doseq [e elements]
				       (db-env-remove-db *db-env* e))))
	   
	   rename-db-and-indices (fn [#^String source #^String destination]
				   (let [pattern (re-pattern (str source ".*"))
					 elements (filter #(re-matches pattern %) 
							  (seq (db-env-db-names *db-env*)))]
				     (doseq [#^String e elements]
				       (db-env-rename-db *db-env* e (.replace e source destination)))))]
       
       
       (remove-db-and-indices cname)
       
       (let [there-are-no #(or (nil? %) (= 0 %))
	     names (names date)
	     get-values (fn [e] (reduce (fn [a b]
					  (assoc a (first b) (second b)))
					(sorted-map)
					e))
	     
	     num (records date)
	     as-vector (fn [m] (reduce (fn [r e]
					 (conj r (key e) (val e)))
				       [] m))]
	 
	 (if (= num (count names))
	   (str date " is already fully compressed")
	   (let [db (open-db cname true)]
	     (try
	       (doseq [e names]
		 (when-not (termination-fn)
		   (when-let [data (get-from-db
				    get-values
				    date
				    e)]
		     (when-let [bytes (to-bytes data)]
		       (db-put (:db db)
			       (index-for-name date e)
			       bytes)))))
	       (finally
		(close-db db)))
	     (locking lockobj
	       (if-not (termination-fn)
		 (if (= num (records date))
		   (if (there-are-no (get @users-per-data date))
		     (do (clean-after-release [date])
			 (remove-db-and-indices (str date))
			 (rename-db-and-indices cname (str date))
			 (str date " has been compressed"))
		     (str "Could not rename " date " after compression. It is being used"))
		   (str "Could not rename after compress " date ". The source was concurrently modified"))
		 (str "Compression of " date " aborted"))))))))
  ([date]
     (compress-data date (fn [] false))))



 


(deftest test-read-and-write
  (let [tmp (make-temp-dir)
	df (SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)
	get-values (fn [e] (reduce (fn [a b]
				      (assoc a (first b) (second b)))
				    (sorted-map)
				    e))
	key1 {:olle "ett" :nisse "chainsaw"}
	key2 {}
	key3 {:saft "skalle"}
	day1 20101123
	day2 20101124]
    (try
      (using-db-env tmp
		    
		    (is (nil? (get-from-db get-values day1 key1)))
		    (add-to-db 3 (dparse "20101123 012233") key1)
		    (is (= {(dparse "20101123 012233") 3}
			 (get-from-db get-values 20101123  key1)))
		    (add-to-db 4 (dparse "20101123 012234") key1)
		    (add-to-db 3 (dparse "20101124 012234") key1)
		    (add-to-db 44 (dparse "20101123 012234") key2)
		    (is (= {(dparse "20101123 012234") 4
			    (dparse "20101123 012233") 3}
			   (get-from-db get-values day1  key1)))
		    (add-to-db 5 (dparse "20101123 012235") key1)

		    (is (= {(dparse "20101123 012234") 4
			    (dparse "20101123 012235") 5
			    (dparse "20101123 012233") 3}
			   (get-from-db get-values day1  key1)))
		    (is (= { (dparse "20101123 012234") 44}
			   (get-from-db get-values day1  key2)))
		    (is (= 4 (records day1)))
		    (compress-data day1)
		    (is (= {(dparse "20101123 012234") 4
			    (dparse "20101123 012235") 5
			    (dparse "20101123 012233") 3}
			   (get-from-db get-values day1  key1)))
		    (is (= { (dparse "20101123 012234") 44}
			   (get-from-db get-values day1  key2)))


		    (is (= 2 (records day1)))
		    (is (= #{key1 key2} (set (names day1))))
		    (remove-date day1)
		    (is (nil? (records day1)))
		    (is (= #{key1} (set (names day2))))
		    (is (nil? (names day1)))
		    (remove-date day1)
		    (is (= {(dparse "20101124 012234") 3}
			   (get-from-db get-values day2 key1)))
		    )
      (finally  (rmdir-recursive tmp)))))
