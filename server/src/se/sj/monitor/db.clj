(ns se.sj.monitor.db
  (:use cupboard.bdb.je)
  (:use cupboard.utils)
  (:use clojure.contrib.profile)
  (:import java.text.SimpleDateFormat))

(declare *db* *db-env* *next-key* *indices*)

(defn incremental-key 
  ([db]
     (let [the-next-key 
	   (atom (with-db-cursor [cur db]
		   (let [last-entry (db-cursor-last cur)]
		     (if (empty? last-entry)
		       0
		       (inc (first last-entry))))))]
       #(dec (swap! the-next-key inc))))
  ([]
     (incremental-key *db*)))

(defn using-db
  [path db-name indexed-keywords f]
  (binding [*db-env* (db-env-open path :allow-create true  :transactional true :txn-no-sync true)]
    (try 
     (binding [*db* (db-open *db-env* db-name :allow-create true)]
       (try 
	(binding [*next-key* (incremental-key *db*)]
	  (binding [*indices* (reduce 
			       #(assoc %1 %2 (db-sec-open *db-env* 
							   *db* 
							   (name %2) 
							   :allow-create true 
							   :sorted-duplicates true
							   :key-creator-fn %2)) 
			       {} indexed-keywords )]
	    (try
	     (f)
	     
	     (finally (dorun (map #(db-sec-close %) (vals *indices*)))))))
	(finally (db-close *db*))))
     (finally (db-env-close *db-env*)))))


(defn for-all-in-main [f]
  "Calls f for each element in f"
  (with-db-cursor [ c *db*]
    (dorun (map #(let [d (second %)
		       time-stamp (:time d)
		       value (:value d)
		       data (dissoc d :time :value :date)]
		     (f [data time-stamp value])) 
		(take-while #(not (empty? %)) 
			    (repeatedly #(db-cursor-next c)))))))

(defn all-in-main 
  "Calls f with a lazy seq of all emlements in main of *db*. The cursor closes after f. That is, seq passed to f is no longer valid after f" 
  [f]
  (with-db-cursor [ c *db*]
    (let [p (fn p [c]
	      (let [n (db-cursor-next c)] 
		(if (empty? n) 
		  nil
		  (let [d (second n)
			time-stamp (:time d)
			value (:value d)
			data (dissoc d :time :value :date)]
		    (lazy-seq (cons [data time-stamp value] (p c)))))))]
      (f (p c)))))


(defn for-all-in [indexed-key begining-with f]
  (if-let [idx (indexed-key *indices*)]
    (with-db-cursor [t idx]
      (db-cursor-search t begining-with)
      (with-db-join-cursor [j [t]]
	(dorun (map #(let [d (second %)
			   time-stamp (:time d)
			   value (:value d)
			   data (dissoc d :time :value :date)]
		       (f [data time-stamp value])) 
		    (take-while #(not (empty? %)) (repeatedly #(db-join-cursor-next j)))))))
    (throw (IllegalArgumentException. (str indexed-key " is not a key in database.")))))


(defn add-to-db 
 ([db next-key value time fields]
    (when (:time fields)
      (throw (IllegalArgumentException. 
	      (str "Fields may not contain a :time field. :time " 
		   (:time fields)))))
    (when (:date fields)
      (throw (IllegalArgumentException. 
	      (str "Fields may not contain a :date field. :date " 
		   (:date fields)))))
    (when (:value fields)
      (throw (IllegalArgumentException. 
	      (str "Fields may not contain a :value field. :value " 
		   (:value fields))))) 
      
    (let [data (assoc fields 
		 :time time 
		 :date (. (SimpleDateFormat. "yyyyMMdd") format time) 
		 :value value)]
      (db-put db (next-key) data)))

 ([data time keys]
    (add-to-db *db* *next-key* data time keys)))


(comment
  (defn brul []
    (time (using-db mydir "adb" (fn [] (dotimes [i 100] (do
							  (add-to-db i (Date.) {:olle "Olle" :nisse "Nisse"})
							  (add-to-db 101 (Date.) {:olle "Olle" :nisse "AnotherNisse"}))))))))