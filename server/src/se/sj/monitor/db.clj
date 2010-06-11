(ns se.sj.monitor.db
  (:use cupboard.bdb.je)
  (:use cupboard.utils)
  (:use [clojure stacktrace test])
  (:use [clojure.contrib profile ])
  (:import java.text.SimpleDateFormat)
  (:import java.util.Date)
  (:import [com.sleepycat.je OperationStatus DatabaseException LockConflictException]))

(def *db* (ref nil))
(def *db-env* (ref nil)) 
(def *next-key* (ref nil))
(def *indices* (ref nil))

(defn- causes [e] 
  (take-while #(not (nil? %)) 
	      (iterate (fn [#^Throwable i] (when i (. i getCause))) 
		       e)))

(defonce date-format "yyyyMMdd")

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
  "Defines the database to use, that will be closed after body. Path is directory path to db store, dn-name is the name of the db, and indexed-keyword are keywords in data that will be indexed."
  [path db-name indexed-keywords & body]
  `(when-let [db-env# (db-env-open ~path  :allow-create true  :transactional true :txn-no-sync true)]
     (try
	(when-let [db# (db-open db-env# ~db-name :allow-create true)]
	  (try
	   (let [next-key# (incremental-key db#)]
	     (when-let [indices# (reduce (fn [m# v#] 
					   (assoc m# v# 
						  (db-sec-open db-env# 
							       db# 
							       (name v#) 
							       :allow-create true 
							       :sorted-duplicates true
							       :key-creator-fn v#))) 
					 {} ~indexed-keywords )]
	       (try

		(dosync
		 (ref-set *db-env* db-env#)
		 (ref-set *db* db#)
		 (ref-set *next-key* next-key#)
		 (ref-set *indices* indices#)
		)
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

(defn- create-structure-as-vector [d]
  (let [time-stamp (:time d)
	value (:value d)
	data (dissoc d :time :value :date)]
     [data time-stamp value]))


;(defn all-in-main 
;  "Calls fun with a lazy seq of all emlements in main of *db*. The cursor closes after fun. That is, seq passed to fun is no longer valid after fun" 
;  [fun]
;  (with-db-cursor [ cursor @*db*]
;    (let [fn-argument (fn internal-fn-argument [c]
;	      (let [next-elem (db-cursor-next cursor)] 
;		(if (empty? next-elem) 
;		  nil
;		  (let [data (create-structure-as-vector (second next-elem))]
;		    (lazy-seq (cons data (internal-fn-argument cursor)))))))]
;      (fun (fn-argument cursor)))))


(defn all-in-every 
  "Returns result of calling fun with a lazy seq of all emlements according to keys-and-data. That is, pairs indexed keywords and expected values. Keywords are distinct. The cursor closes after fun. That is, seq passed to fun is no longer valid after fun" 
  [fun & keys-and-data] 
  (let [indexes-and-data (apply hash-map keys-and-data)
	is-valid-indexes #(reduce (fn [b index] 
				    (if (contains? @*indices* index) 
				      b 
				      false)) 
				  true 
				  (keys indexes-and-data))
	close-cursor #(db-cursor-close %)
	close-cursors #(dorun (map close-cursor %))
	open-cursor (fn [s index-and-data]
		      (let [cursor (db-cursor-open ((key index-and-data) @*indices*) :isolation :read-uncommited)]
			(try (if (empty? (db-cursor-search cursor (val index-and-data) :exact true))
			       (do (close-cursor cursor) 
				   s)
			       (conj s cursor))
			     (catch Exception e 
			       (close-cursors (conj s cursor))
			       (throw e)))))]
    (when (is-valid-indexes)
      (let [failed (atom true)
	    retries (atom 0)
	    r (atom nil)]
     (while (and @failed (> 500 @retries))
	     (try
	      (let [cursors (reduce open-cursor [] indexes-and-data)]
		(try
		 (when (and (= (count cursors) (count indexes-and-data)) (not (empty? cursors)))
		   (with-db-join-cursor [join cursors]
		     (let [p (fn p [jo]
			       (let [ne (db-join-cursor-next jo)]
				 (if (empty? ne)
				   nil
				   (let [data (create-structure-as-vector (second ne))]
				     (lazy-seq (cons data (p jo)))))))]
		       (reset! r (fun (p join))))))
		 (finally (dorun (map #(db-cursor-close %) cursors))))
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
      
    (let [data (assoc keys 
		 :time time 
		 :date (. (SimpleDateFormat. date-format) format time) 
		 :value value)
	  failed (atom true)
	  retries (atom 0)]
      
      (while (and @failed (> 500 @retries))  
	     (try
	      (db-put db (next-key) data)
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


(defn remove-from-db 
  "Removes all entries in db where index indexKey in current set of indices is strictly matching value"
  [indexKey value]
  (when-let [index (indexKey @*indices*)]
    (db-sec-delete index value)))


(defn remove-spec-from-db [date];as long
   (with-db-txn [t @*db-env*]
     (try
      (with-db-cursor [c @*db* :txn t]
       (let [result (atom false)]
	 (dotimes [q 10]
	   (let [n (db-cursor-next c)]
	     (if (empty? n)
	       (reset! result false)
	       (let [data (second n)]
		 (if (< (Long/parseLong (:date data)) date)
		   (do (db-cursor-delete c)
		       (reset! result true))
		   (reset! result false))))))
	@result)))
   (catch Exception e 
     (if (some #(instance? LockConflictException %) (causes e))
       (do
	 (db-txn-abort t)
	 (println (str (java.util.Date.) " deadlock in remove"))
	 (try (Thread/sleep 500) (catch Exception e))
	 true)
       (throw e)))))

;(defn remove-until-from-db 
;"Removes all entries that index indexKey, in current set of indices, is currently less than value"
;[indexKey value]
;  (when-let [index (indexKey @*indices*)]
;    (with-db-txn [txn @*db-env*]
;      (with-db-cursor [cursor index :txn txn]
;	(while (let [record  (do (db-cursor-first cursor))]
;		 (if (empty? record)
;		   nil
;		   (if (< 0 (.compareTo value (indexKey (second record))))
;		     (do
;		       (db-cursor-delete cursor)
;		       true)
;		     nil))))))))

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

