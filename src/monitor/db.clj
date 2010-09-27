(ns monitor.db
  (:use cupboard.bdb.je)
  (:use cupboard.utils)
  (:use [clojure stacktrace test])
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
(def dayname-db (atom nil))
(def dayname-cache (atom nil))
(def all-days (atom #{}))

(defn- time-stamp-of [bytes]
  (.getLong (ByteBuffer/wrap bytes)))

(defn- host-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 16))


(defn- category-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 20))


(defn- counter-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 24))


(defn- instance-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 28))
   

(def index-keywords-and-creators (zipmap 
			       [:host :category :counter :instance]
			       [host-index-of 
				category-index-of 
				counter-index-of 
				instance-index-of]))

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

(defn- name-for-index [day index]
  (get (:by-index (get-day day)) index))

(defn- save-index [days day]
  (when @dayindex-db
    (when-let [current (get days day)]
      (db-put @dayindex-db day (:by-index current)))))

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



(defn- to-bytes [day value time-stamp index-data]
  (let [array-stream (ByteArrayOutputStream.)
	data-out (DataOutputStream. array-stream)
	the-day (get-day day)]

    (.writeLong data-out (.getTime #^Date time-stamp))
    (.writeDouble data-out (double value))
    (.writeInt data-out (index-for-name day (:host index-data)))
    (.writeInt data-out (index-for-name day (:category index-data)))
    (.writeInt data-out (index-for-name day (:counter index-data)))
    (.writeInt data-out (index-for-name day (:instance index-data)))
    (let [others (count (dissoc index-data :host :category :counter :instance))]
      ;(println "*<-" index-data "*" others)
      (.writeByte data-out others)
      (when (< 0 others)
	(dorun (map (fn [e] 
		      (.writeInt data-out (index-for-name day (name (key e))))
		      (.writeInt data-out (index-for-name day (val e))))
		    (dissoc index-data :host :category :counter :instance)))))
    (.close data-out)
    (.toByteArray array-stream)))


(defn- from-bytes [bytes day]
  (let [buffer (ByteBuffer/wrap bytes)
	timestamp (Date. (.getLong buffer))
	value (.getDouble buffer)
	host (name-for-index day (.getInt buffer))
	category (name-for-index day (.getInt buffer))
	counter (name-for-index day (.getInt buffer))
	instance (name-for-index day (.getInt buffer))
	additional (int (.get buffer))
	r (reduce #(if (second %2)
		     (assoc %1 (first %2) (second %2))
		     %1)
		  {}
		  [[:host host] 
		   [:counter counter] 
		   [:category category] 
		   [:instance instance]])]
;	r (assoc {} 
;	    :host host 
;	    :counter counter 
;	    :category category 
;	    :instance instance)]
    
    (let [res (if (< 0 additional)
      (loop [i 0 v r]
	(if (< i additional)
	  (recur (inc i) 
		 (assoc v 
		   (keyword (name-for-index day (.getInt buffer))) 
		   (name-for-index day (.getInt buffer))))
	  [v timestamp value]))
      [r timestamp value])]
      ;(println "*->" res "*" additional)
      res)))

(defn- causes [e] 
  (take-while #(not (nil? %)) 
	      (iterate (fn [#^Throwable i] (when i (. i getCause))) 
		       e)))



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
  `(when-let [db-env# (db-env-open ~path  :allow-create true  :transactional true :txn-no-sync true)]
     (when-let [dayindex-db# (db-open db-env# "day-index" :allow-create true)]
       (when-let [dayname-db# (db-open db-env# "day-name" :allow-create true)]
       (reset! dayindex-db dayindex-db#)
       (reset! current-day-indexes {})
       (reset! lru-days-for-indices [])
       (reset! dayname-db dayname-db#)
       (swap! all-days (fn [c#] into c# (all-dates)))
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
	   (reset! current-day-indexes  {})
	   (reset! dayindex-db nil)
	   (reset! lru-days-for-indices [])
	   (reset! dayname-db nil)
	   (evict-dayname-cache)
	   (db-close dayname-db#)
	   (db-close dayindex-db#)
	   (dosync
	    (ref-set opened-data {})
	    (ref-set unused-data-queue '())
	    (ref-set users-per-data {})
	    (ref-set keep-open-data #{})
	  )))
       (finally
	(dosync (ref-set *db-env* nil))))
       
      (finally 
       (db-env-close db-env#)))))))


;Bara användas av aquire
(defn- open-db [db-name]
  (when-let [db (db-open @*db-env* (str db-name) :allow-create true)]
    (let [next-key (incremental-key db)]
      (when-let [indices (reduce (fn [res data]
				   (assoc res 
				     (key data)
				     (db-sec-open @*db-env*
						  db
						  (str db-name "-" (name (key data)))
						  :allow-create true
						  :sorted-duplicates true
						  :key-creator-fn (val data))))
				 {} index-keywords-and-creators)]
	{:db db, :name db-name, :next next-key, :indices indices}))))



(defn close-db [db]
  (dorun (map (fn [v] (db-sec-close v)) (vals (:indices db))))
  (db-close (:db db)))



(defn aquire-db 
  ([day retries]
     (let [day (day-as-int day)
	   do-aquire-db (fn [day]
			  (dosync
			   (let [db (get @opened-data day)]
			     (if (not db)
			       (do (swap! retries #(dec %))
				   nil)
			       (do
				 (alter unused-data-queue (fn [c] (remove #(= day %) c)))
				 (alter users-per-data assoc day (inc (get @users-per-data day 0)))
				 db
				 )))))
       	   db (if (not (get @opened-data day))
		(let [a-db (open-db day)
		      already-added (atom false)
		      db (dosync 
			  (alter opened-data (fn [o]
					      (if (get o day)
						(do (swap! already-added (fn [_] true))
						    o)
						(assoc o day a-db))))
			  (do-aquire-db day))]
		  (when @already-added 
		    (close-db a-db))
		  db)
		(do-aquire-db day))]
       (if (and (nil? db) (< 0 retries))
	 (recur day (dec retries))
	 db)))
  ([day]
     (aquire-db day 10)))
      



(defn- clean-after-release []
  (let [should-close (atom '())]
  (dosync
     (when-let [to-close (nthnext @unused-data-queue (- datas-to-keep-alive (count @keep-open-data)))]
       (dorun (map (fn [i] 
		     (swap! should-close #(if-let [h (get @opened-data i)]
					    (conj % h)
					    %)))
		     to-close))
       (alter opened-data #(apply dissoc % to-close))
       (alter unused-data-queue #(apply list (take (- datas-to-keep-alive (count @keep-open-data)) %)))
       ))
  (dorun (map #(close-db %) @should-close))))


(defn release-db [db-name]
    (let [db-name (cond 
		 (= String (class db-name)) (day-as-int db-name)
		 (instance? Number db-name) (day-as-int db-name)
		 (and (associative? db-name) (:name db-name)) (day-as-int (:name db-name))
		 :else (throw (IllegalArgumentException.)))]
		  

    (dosync
     (alter users-per-data assoc db-name (dec (get @users-per-data db-name 1)))
     (when (= 0 (get @users-per-data db-name))
       (when (not (some #{db-name} @keep-open-data))
	 (alter unused-data-queue conj db-name))
       (alter users-per-data dissoc db-name))
     (clean-after-release))))
    
       
(defn mark-as-always-opened [db-name]
  (let [db-name (cond 
		 (= String (class db-name)) (day-as-int db-name)
		 (instance? Number db-name) (day-as-int db-name)
		 (and (associative? db-name) (:name db-name)) (day-as-int (:name db-name))
		 :else (throw (IllegalArgumentException.)))]
     (dosync (alter keep-open-data #(conj % db-name)))))

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

(defmacro using-day [day & body]
  `(when-let [day# (day-as-int ~day)]
     (let [db# (aquire-db day#)]
       (binding [*day* day#
		 *db-struct* db#]  
       (try
	(do ~@body)
	(finally
	 (release-db day#)
	 ))))))


(defn evict-dayname-cache []
  (reset! dayname-cache nil))

(defn- add-to-dayname [date names]
  (when-let [db  @dayname-db]
    (swap! dayname-cache (fn [_] [date names]))
    (db-put @dayname-db date names)
    (swap! all-days (fn [c] (conj c date)))))

(defn get-from-dayname [date]
  (when-let [db @dayname-db]
    (if (= date (first @dayname-cache))
      (second @dayname-cache)
      (second (db-get db date)))))

(defn all-dates []
  (when-let [db @dayname-db]
    (with-db-cursor [cur db]
      (loop [r (sorted-set) c (db-cursor-next cur)]
	(if (empty? c)
	  r
	  (recur (conj r (first c)) (db-cursor-next cur)))))))

; ta bort från data-db osså
(defn remove-date [date]
  (let [date (day-as-int date)
	there-are-no #(or (nil? %) (= 0 %))]
    (when (not (get @keep-open-data date))
      (dosync
       (let [active-users (get @users-per-data date)]
	
	 (when (there-are-no active-users)
	   (swap! all-days disj date)
	   (evict-dayname-cache)
	   (when-let [db (get @opened-data date)]
	     (try
	      (close-db db)
	      (catch Exception e
		(println "Ignoring exception while closing" date)
		(print-stack-trace e))))
	   
	   (alter opened-data dissoc date)
	   (alter unused-data-queue (fn [q] (apply list (remove #(= date %) q))))
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
	    (db-delete @dayname-db date)
	    (db-delete @dayindex-db date)
	    (catch Exception e
	      (println "Ignoring exception while deleteing metadata for" date)
	      (print-stack-trace e)))))))))
	      


(defn- ensure-name [name date]
  (let [date (day-as-int date)]
    (if-let [current (get-from-dayname date)]
      (if (not (contains? current name))
	(add-to-dayname date (conj current name)))
      (add-to-dayname date (conj #{} name)))))

     
(defn get-from-db [fun day & keys-and-data]
  (let [day (day-as-int day)]
    (when (get @all-days day)
      (using-day day
   
		 (let [indexes-and-data (apply hash-map keys-and-data)
		       is-valid-indexes (filter (complement (set (keys index-keywords-and-creators)))
						(keys indexes-and-data))
		       close-cursor #(db-cursor-close %)
		       close-cursors #(dorun (map close-cursor %))
		       open-cursor (fn [opened-cursors index-and-data]
				     (let [cursor (db-cursor-open ((key index-and-data) (:indices *db-struct*)) :isolation :read-uncommited)]
				       (try
					(let [index-of-name (index-for-name *day* (val index-and-data))]
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
						    (lazy-seq (cons (from-bytes (second ne) *day*) (p jo))))))]
					(reset! r (fun (p join))))))
				  (finally (close-cursors cursors)))
				 (reset! failed false))
			       (catch Exception e
				 (if (some #(instance? LockConflictException %) (causes e))
				   (do (swap! retries (fn [c] (inc c))) 
				       (println (str (java.util.Date.) " deadlock in read"))
				       (try (Thread/sleep 500) (catch Exception e)))
				   (throw e)))))
	  @r)))))))


  
(defn add-to-db 
  "Adds an entry to db. There will be an index of day-stamps if index :date is currently in current set of inidices. keyword-keys in keys will be indices, if those inidices is currently in use."
  [value time keys]
  (using-day (day-as-int time)
	     (let [data (to-bytes *day* value time keys)
		   failed (atom true)
		   retries (atom 0)]
	       (ensure-name keys *day*)
	       (while (and @failed (> 500 @retries))  
		      (try
		       (db-put (:db *db-struct*) 
			       (.array (.putLong (ByteBuffer/allocate 8) ((:next *db-struct*))))
			       data)
		       (reset! failed false)
		       (catch Exception e 
			 (if (some #(instance? LockConflictException %)(causes e))
			   (do (swap! retries (fn [c] (inc c)))
			       (println (str (java.util.Date.) " deadlock in add"))
			       (try (Thread/sleep 500)
				    (catch Exception e)))
			   (throw e))))))))


 


(deftest test-db
(binding [datas-to-keep-alive 3]
  (let [tmp (make-temp-dir)
	df (SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)
	printod (fn printod []
		  (println "....start")
		  (when (not (=(count (keys @opened-data)) (count (set (vals @opened-data)))))
		    (throw (IllegalStateException. "What*******************")))
		  (println "opened-data" (keys @opened-data))
		  (println "unused-data-queue" @unused-data-queue)
		  (println "users-per-data" @users-per-data)
		  (println "keep-open-data" @keep-open-data)
		  (println "....end")
		  )]
    (try
     (using-db-env tmp

		   (doall (add-to-db 2 (dparse "20070101 120001") {:category "Olof" :counter "Gustav" :instance "Nisse" :host "Adam"})			    )
		   (add-to-db 3 (dparse "20070101 120001") {:category "Olof" :counter "Gustav" :instance "Nisse" :host "Adam"})
		   (add-to-db 4 (dparse "20070101 120001") {:category "Olof" :counter "Gustav" :instance "Nisse" :host "Adam"})
		   (add-to-db 5 (dparse "20070101 120001") {:category "Olof" :counter "Gustav" :instance "Nisse" :host "Bertil"})
		   (add-to-db 6 (dparse "20070101 120001") {:category "Olof" :counter "Gustav" :instance "Nisse" :host "Bertil"})
		   (add-to-db 7 (dparse "20070101 120001") {:category "Olof" :counter "Gustav" :instance "Nisse"})
		   (add-to-db 8 (dparse "20070102 120001") {:category "Olof" :counter "Gustav" :instance "Nisse" :host "Adam"})		   

			    (let [db (aquire-db 20070101)]
			      (is (= 6 (db-count (:db db))))
			      (release-db 20070101))
			    (is (= [2 3 4]
				 (doall (get-from-db 
				  #(reduce (fn [a b]
					     (conj a (nth b 2))) [] %) 
				  20070101 
				  :category "Olof" :counter "Gustav" :host "Adam" ))))

			    (is (= [8]
				 (doall (get-from-db 
				  #(reduce (fn [a b] (conj a (nth b 2))) [] %) 
				  20070102 
				  :category "Olof" :counter "Gustav" :host "Adam" ))))

			    (is (= [8]
				 (doall (get-from-db 
				  #(reduce (fn [a b] (conj a (nth b 2))) [] %) 
				  20070102 
				  :category "Olof" :counter "Gustav" :host "Adam" ))))


			    (is (nil? (doall (get-from-db 
					   #(reduce (fn [a b] (conj a (nth b 2))) [] %) 

				      20070103 
				  :category "Olof" :counter "Gustav" :host "Adam" ))))

			    (is (nil? (doall (get-from-db 
					   #(reduce (fn [a b] (conj a (nth b 2))) [] %) 

				      20070101 
				  :category "Guilla" :counter "Gustav" :host "Adam" ))))

			    (is (= #{20070101 20070102} (set (all-dates))))
			    (is (= #{{:instance "Nisse", :counter "Gustav", :category "Olof"} 
				     {:host "Adam",   :instance "Nisse", :counter "Gustav", :category "Olof"} 
				     {:host "Bertil", :instance "Nisse", :counter "Gustav", :category "Olof"}}
				     (get-from-dayname 20070101)))
			    (is (= #{{:category "Olof", :counter "Gustav", :instance "Nisse", :host "Adam"}}
				     (get-from-dayname 20070102)))
			    (is (= #{20070101 20070102} (set (days-with-data))))
			    (is (= #{20070101 20070102} @all-days))
			    (remove-date 20070102)
			    (is (= #{20070101} (set (days-with-data))))
			    (is (nil?
				 (doall (get-from-db 
					 #(reduce (fn [a b] (conj a (nth b 2))) [] %) 
					 20070102 
					 :category "Olof" :counter "Gustav" :host "Adam" ))))
			    (is (= [2 3 4]
				 (doall (get-from-db 
				  #(reduce (fn [a b]
					     (conj a (nth b 2))) [] %) 
				  20070101 
				  :category "Olof" :counter "Gustav" :host "Adam" ))))

			    (using-day 20070101
				       (remove-date 20070101))
			    (is (= [2 3 4]
				   (doall (get-from-db 
					   #(reduce (fn [a b]
						      (conj a (nth b 2))) [] %) 
					   20070101 
					   :category "Olof" :counter "Gustav" :host "Adam" ))))

			    (is (= #{20070101} (set (days-with-data))))
			    (remove-date 20070101)
			    (is (= #{} (set (days-with-data))))
			    (is (nil?
				 (doall (get-from-db 
					 #(reduce (fn [a b]
						    (conj a (nth b 2))) [] %) 
					 20070101 
					 :category "Olof" :counter "Gustav" :host "Adam" ))))
			    (remove-date 20070101)

			    (add-to-db 5 (dparse "20070101 120001") {:category "Olof" :counter "Gustav" :instance "Nisse" :host "Bertil"})
			    (is (= #{20070101} (set (days-with-data))))
			    (is (nil?
				   (doall (get-from-db 
					   #(reduce (fn [a b]
						      (conj a (nth b 2))) [] %) 
					   20070101 
					   :category "Olof" :counter "Gustav" :host "Adam" ))))
			    (is (= [5]
				   (doall (get-from-db 
					   #(reduce (fn [a b]
						      (conj a (nth b 2))) [] %) 
					   20070101 
					   :host "Bertil" ))))
			    (remove-date 20070101)
			    (is (= #{} (set (days-with-data))))

			    
		   )
     (using-db-env tmp
		  

		  
		  (let [adb (open-db "Arne")
			adb2 (open-db "Arne")] 
		    (try 
		     (try
		      (finally
		       (close-db adb)
		       ))
		     (finally
		      (close-db adb2)))
		      )
		  

		  (let [adb (aquire-db 20070812)
			abd2 (aquire-db "20070812")]
		    (try
		     (try
		      (is (= 2 (get @users-per-data 20070812)))
		      (is (= 1 (count @opened-data)))
		      (finally 
		       (release-db "20070812")))
		     (is (= 1 (get @users-per-data 20070812)))
		     (is (= 1 (count @opened-data)))
		     (finally
		      (release-db 20070812)))
		    (is (nil? (get @users-per-data 20070812)))
		    (is (= 1 (count @opened-data))))
		  
)


     (using-db-env tmp
		   (binding [datas-to-keep-alive 4]
		   (let [dbs (doall (map #(aquire-db (format "200711%02d" %)) (range 7)))]
		     (is (= (set (range 20071100 20071107)) (set (keys @opened-data))))
		     (dorun (map #(release-db (:name %)) dbs))
		     (is (= (set (range 20071103 20071107)) (set (keys @opened-data))))
		     )))


     (using-db-env tmp
		   (binding [datas-to-keep-alive 4]
		   
		   (let [dbs (doall (map #(aquire-db (format "200712%02d" %)) (range 7)))]
		     (mark-as-always-opened 20071201)
		     (is (= (set (range 20071200 20071207)) (set (keys @opened-data))))
		     (dorun (map #(release-db (:name %)) dbs))
		     (is (= (conj (set (range 20071204 20071207)) 20071201) (set (keys @opened-data))))
		       (is (= (count @opened-data) 4))
		       (is (= 1 (count @keep-open-data)))
		       (is (= 3 (count @unused-data-queue)))
		       (is (zero? (count @users-per-data)))


		     (mark-as-not-always 20071201)
		     (is (= (count @opened-data) 4))
		     (is (zero? (count @keep-open-data)))
		     (is (= 4 (count @unused-data-queue)))
		     (is (zero? (count @users-per-data)))


		     )))

     (using-db-env tmp
		   (binding [datas-to-keep-alive 4]
		       (mark-as-always-opened 20071201)
		     (let [dbs (doall (map #(aquire-db (format "200712%02d" %)) (range 7)))]
		       (is (= (count @opened-data) 7))
		       (is (= 1 (count @keep-open-data)))
		       (is (zero? (count @unused-data-queue)))
		       (is (= 7 (count @users-per-data)))
		       (is (= 7 (apply + (vals @users-per-data))))
		       
		       (mark-as-not-always 20071201)
		       (is (= (count @opened-data) 7))
		       (is (zero? (count @keep-open-data)))
		       (is (zero? (count @unused-data-queue)))
		       (is (= 7 (count @users-per-data)))
		       (is (= 7 (apply + (vals @users-per-data))))

		       (dorun (map #(release-db (:name %)) dbs))
		       (is (= 4 (count @opened-data)))
		       (is (zero? (count @keep-open-data)))
		       (is (= 4 (count @unused-data-queue)))
		       (is (zero? (count @users-per-data)))
		       (is (zero? (apply + (vals @users-per-data)))))))


     (using-db-env tmp
		   (binding [datas-to-keep-alive 4]
		     (mark-as-always-opened 20071201)
		     (let [dbs (into (doall (map #(aquire-db (format "200712%02d" %)) (range 3)))
				     (doall (map #(aquire-db (format "200712%02d" %)) (range 3))))]
		       (is (= 3 (count @opened-data)))
		       (is (= 1 (count @keep-open-data)))
		       (is (zero? (count @unused-data-queue)))
		       (is (= 3 (count @users-per-data)))
		       (is (= 6 (apply + (vals @users-per-data))))
		       (mark-as-not-always 20071201)
		       (is (= 3 (count @opened-data)))
		       (is (zero? (count @keep-open-data)))
		       (is (zero? (count @unused-data-queue)))
		       (is (= 3 (count @users-per-data)))
		       (is (= 6 (apply + (vals @users-per-data))))
		       (release-db 20071201)
		       (is (= 3 (count @opened-data)))
		       (is (zero? (count @keep-open-data)))
		       (is (zero? (count @unused-data-queue)))
		       (is (= 3 (count @users-per-data)))
		       (is (= 5 (apply + (vals @users-per-data))))
		       (release-db 20071201)
		       (is (= 3 (count @opened-data)))
		       (is (zero? (count @keep-open-data)))
		       (is (= 1 (count @unused-data-queue)))
		       (is (= 2 (count @users-per-data)))
		       (is (= 4 (apply + (vals @users-per-data))))
		       (mark-as-always-opened 20071201)
		       (is (= 3 (count @opened-data)))
		       (is (= 1 (count @keep-open-data)))
		       (is (= 1 (count @unused-data-queue)))
		       (is (= 2 (count @users-per-data)))
		       (is (= 4 (apply + (vals @users-per-data))))
		       (dotimes [_ 2](release-db 20071200))
		       (dotimes [_ 2](release-db 20071202))
		       (is (= 3 (count @opened-data)))
		       (is (= 1 (count @keep-open-data)))
		       (is (= 3 (count @unused-data-queue)))
		       (is (= 0 (count @users-per-data)))
		       (is (= 0 (apply + (vals @users-per-data))))
		       )))
     (finally (rmdir-recursive tmp))))))


(deftest testindices
  (reset! current-day-indexes {})
  (reset! lru-days-for-indices [])
  (let [df (SimpleDateFormat. "yyyyMMdd HHmmss")]

      (let [data (assoc {} :host "1" :counter "3" :category "2" :instance "4")]
	
	(let [bytes (to-bytes 20071126 23.0 (.parse df "20071126 033332") data)]
	  (is (= (.parse df "20071126 033332") (Date. (time-stamp-of bytes))))
	  (is (= 0 (host-index-of bytes)))
	  (is (= 1 (category-index-of bytes)))
	  (is (= 2 (counter-index-of bytes)))
	  (is (= 3 (instance-index-of bytes)))
	  (is (= data (first (from-bytes bytes 20071126))))
	  (is (= (reduce #( conj %1 (name-for-index 20071126 %2)) [] (range 4) )
		 ["1" "2" "3" "4"]))
	  (is (= (reduce #( conj %1 (index-for-name 20071126 %2)) [] ["1" "2" "3" "4"] )
		 (range 4)))
	   ))
      

      (let [data (assoc {} :host "1" :counter "3" :category "2" :instance "4" :oljemagnat "magnaten")]
	(let [bytes (to-bytes 20071127 23.0 (.parse df "20071127 033332") data)]
	  (is (= data (first (from-bytes bytes 20071127))))
	  (is (= (reduce #( conj %1 (name-for-index 20071127 %2)) [] (range 6) )
		 ["1" "2" "3" "4" "oljemagnat" "magnaten"]))
	  (is (= (reduce #( conj %1 (index-for-name 20071127 %2)) [] ["1" "2" "3" "4" "oljemagnat" "magnaten"] )
		 (range 6)))))))


(deftest testindices-with-persistence
  (let [tmp (make-temp-dir)]
    (try
     (let [df (SimpleDateFormat. "yyyyMMdd HHmmss")]
       
       (let [data (assoc {} :host "1" :counter "3" :category "2" :instance "4")
	     another-data (assoc {} :host "1" :counter "3" :category "2" :instance "4")
	     some-more-data (assoc {} :host "1" :counter "33" :category "2" :instance "44")
	     even-more-data (assoc {} :host "1" :counter "33" :category "2" :instance "44" :gurkmeja "meja")]
	 (using-db-env tmp
		       (let [bytes (to-bytes 20071126 23 (.parse df "20071126 033332") data )] 
			 (is (= data (first (from-bytes bytes 20071126))))
			 (is (= 23.0 (nth (from-bytes bytes 20071126) 2)))
			 (let [another-bytes (to-bytes 20071125 2 (.parse df "20071125 033332") another-data )]
			   (is (= 23.0 (nth (from-bytes bytes 20071126) 2)))
			   (is (= 2.0 (nth (from-bytes another-bytes 20071125) 2)))
			   (is (= 23.0 (nth (from-bytes bytes 20071126) 2)))
			   (is (= another-data (first (from-bytes another-bytes 20071125))))
			   (let [some-more-bytes (to-bytes 20071125 3.0 (.parse df "20071125 033332") some-more-data )]
			     (is (= 2.0 (nth (from-bytes another-bytes 20071125) 2)))
			     (is (= 3.0 (nth (from-bytes some-more-bytes 20071125) 2)))
			     (is (= 23.0 (nth (from-bytes bytes 20071126) 2)))
			     (is (= 2.0 (nth (from-bytes another-bytes 20071125) 2)))
			     (is (= 3.0 (nth (from-bytes some-more-bytes 20071125) 2)))
			     (is (= #{0 1 2 3 4 5} 
				    (set (for [x [bytes another-bytes some-more-bytes] 
					       y [host-index-of category-index-of counter-index-of instance-index-of]]
					   (y x)))))
			     (is (= #{0 1 2 3} 
				    (set (for [x [another-bytes bytes] 
					       y [host-index-of category-index-of counter-index-of instance-index-of]]
					   (y x)))))
			     (is (= some-more-data (first (from-bytes some-more-bytes 20071125))))
			     (let [even-more-bytes (to-bytes 20071125 3.0 (.parse df "20071125 033332") even-more-data)]
			       (is (= even-more-data (first (from-bytes even-more-bytes 20071125))))))))
		       (is (= 6 (index-for-name 20071125 "gurkmeja")))
		       (reset! current-day-indexes {})
		       (reset! lru-days-for-indices [])
		       (let [some-more-bytes (to-bytes 20071125 3.0 (.parse df "20071125 033332") some-more-data )]
			 (is (= 6 (index-for-name 20071125 "gurkmeja")))
			 (is (every? 
			      (reduce #(conj %1 (%2 some-more-bytes)) 
				      #{} 
				      [host-index-of category-index-of counter-index-of instance-index-of])
			      #{4 5}))
			 (is (= some-more-data (first (from-bytes some-more-bytes 20071125))))
			 (let [bytes (to-bytes 20071126 23 (.parse df "20071126 033332") data )]
			   (is (= data (first (from-bytes bytes 20071126))))
			   (is (= 23.0 (nth (from-bytes bytes 20071126) 2))))
			 
			 ))))
     (finally (rmdir-recursive tmp)))))

(deftest dayname-test
  (let [tmp (make-temp-dir)
	df (SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (try
     (using-db-env tmp 
		   (is (nil? (get-from-dayname 20071120)))
		   (add-to-dayname 20071120 "Nisse")
		   
		   (is (= "Nisse" (get-from-dayname 20071120)))
		   (add-to-dayname 20071120 "Olle")
		   (is (= "Olle" (get-from-dayname 20071120)))
		   (add-to-dayname 20071120 #{{:a "Hum" :b "Di"} {:a "Ni" :b "Di"}})
		   (is (= #{{:a "Hum" :b "Di"} {:a "Ni" :b "Di"}} (get-from-dayname 20071120)))
		   (is (nil? (get-from-dayname 20071121)))
		   (let [all (all-dates)]
		     (is (= 1 (count all)) "Only one")
		     (is (= 20071120 (first all)))
		     (remove-date (first all))
		     (is (empty? (all-dates))))
		   (add-to-dayname 20071120 "Olle")
		   (add-to-dayname 20071121 "Nisse")
		   (add-to-dayname 20071122 "Arne")
		   (let [all (all-dates)]
		     (is (= 3 (count all)))
		     (is (= 20071121 (second all)))) 
		   
		   (remove-date 20071121)
		   (let [all (all-dates)]
		     (is (= 2 (count all)))
		     (is (= 20071122 (second all)))))
     (finally  (rmdir-recursive tmp)))))