(ns monitor.newdb
  (:import (java.util Date))
  (:import (java.text SimpleDateFormat))
  (:import (java.io IOException File))
  (:import (jdbm RecordManagerFactory RecordManager))
  (:import (jdbm.btree BTree))
   (:import (jdbm.helper Tuple))
  (:use clojure.test)
  (:use [clojure.pprint :only [pprint]])
  (:use monitor.tools))

(comment
  En db per dag, för snabb delete, och manuell patchning, och förhindra defrag behov
  Varje dag är en recman
  Varje dag har "Keys" med recid som nyckel, och map av string-string. RecId är unik. Värden för samma data kan sparas i flera tabeller. Vi kan uppdatera i en annan BTree när vi har en browser
  Data är BTree för ett namn, och har timestamp-value. Data har inte namn. 
  En Data kan skrivlåsas för att supporta TupleBrowser
  Data cacheas, ev till annan db, under låsning. TupleBrowser kan användas långsamt. Sissta tupplebrowsern skriver till rätt data.
  En dag tas bort när ingen har åtkomst. Ev file del, med stängd recman.
  Gamla recmans stängs automatiskt, för att inte ta upp cache ?
  )


(defprotocol DayAsInt
  (as-int [this]))

(extend-protocol DayAsInt
  String (as-int [this] (Integer/parseInt this))
  Number (as-int [this] (int this))
  Date (as-int [this] (Integer/parseInt (.format (SimpleDateFormat. "yyyyMMdd") this))))

(defn- date-as-day
  ([date]
     (if date
       (as-int date)
       (as-int (Date.)))) 
  ([] (date-as-day nil)))

(deftype Browser [closefn seqfn]
  java.io.Closeable
  (close [this]
	 (closefn))
  clojure.lang.Seqable
  (seq [this]
       (seqfn)))

(defn- new-day ;[path day]
  
  [recman lock name-tree ids-tree name-ids]
  {:recman recman
   :lock lock
   :primary-name-tree name-tree
   :primary-ids-tree ids-tree
   :name-ids (ref name-ids)
   :browser-recid (ref {})
   :written-recid (ref nil)})

(defn db-struct [path]
  {:path path ;The directory where db is
   :days-to-keep (atom 10)
   :dayname-days (ref {})
   :day-record-managers (ref {}) ;day-int and record-manager
   :unused-days-lru (ref []) ; unused day-ints lru, head will be removed. **Test that this is ok**
   :day-users (ref {}) ;number of current users for each day-int
   :recman-primary-name-trees (ref {}) ;Btrees to update and initially read recid-names. One for each recman
   :recman-primary-ids-trees (ref {}) ;Btrees to update and initially read recids for a primary recid. One for each recman.
   :date-name-ids (ref {}) ; maps of name and recids (primary and following) for different dates.  
   :lock (ref {}) ;lock per recman, for sensitive things. Guess we should use e.g. Reentrant 
   :browsed-ids-per-recid (ref {}) ;number of browsers per recid for each recman. A writer should create a new id if current is browsed. The recid is cleared when ever a new writer recid is created. That is, its ok if a browser never reads to end.
   :written-ids-per-recid (ref {}) ;written recid per recman, A writer to a recman, should hold a lock, wich the browser can wait on
   })

(comment
  The last recid have to be locked for writing if it is browsed. A writer usually creates a new last recid, and can hence clear the all other browser locks in the sequence.
  The last recid have to be locked for browsing if it is written to. A browser usually waits until the write is complete until it tries to create a browser for the last recid.
  This means that we only care about locking the last recid. All browser locks are cleared when a new writer recid is created
  Browser will leave browse lock if not browsed to end. Does this mean that each client should be able to manually delete its locks- Cumbersome since browsing is made through seq. Perhaps a lock should contain a id to its user, Invalidating browsers when released, or when client leaves the database. Guess that a browser should be something more advanced, being a seqable that is also closeable, Either autoclosed on end, or able to follow writes in rt.  )
  

(defn- browse
  "lazy seq of [time value] for ids"
  [db-struct recman & ids]
  
  (let [prevent-concurrent-writes
	(fn [nid] (when-let [is-written (dosync (alter (:browsed-ids-per-recid db-struct) update-in [recman nid] #(when %
														   (inc %)
														   1))
					     (= nid (get recman @(:written-ids-per-recid db-struct))))]
		 (when-let [lockobj  (get recman @(:lock db-struct))]
		   (locking lockobj
		     (when (= nid (get recman @(:written-ids-per-recid db-struct)))
		       (throw  (IllegalStateException. "There is a nother writer while I have lock!!")))))))
	
	allow-writes (fn [nid]
		       (dosync
			(when-let [for-recman (get @(:browsed-ids-per-recid db-struct) recman)]
			  (if (> 2 (get for-recman nid))
			    (alter (:browsed-ids-per-recid db-struct) assoc recman (dissoc for-recman nid))
			    (alter (:browsed-ids-per-recid db-struct) update-in [recman nid] dec)))))
	f (fn f [browser ids]
	    (let [tuple (Tuple.)]
	      (if (.getNext browser tuple)
		(lazy-seq (cons [(.getKey tuple) (.getValue tuple)] (f browser ids)))
		(when-let [nnid (first ids)]
		  (do
		    
		  (let [browser (.browse (BTree/load recman nnid))]
		    (recur browser (next ids)))))))
	    )
	]
    (when-let [nid (first ids)]
      (prevent-concurrent-writes nid)
      (try
	(f (.browse (BTree/load recman nid)) (next ids))
	(catch Exception e
	  (allow-writes nid)
	  (throw e))))))

(defn browser [db-struct recman & ids]
  (Browser. #(println "Close")
	    #(do (println "Calling browse" recman ids)
		 (apply browse db-struct recman ids))))
   
(defn- read-name-ids "in memory map of name and ids"
  [recman primary-name-tree primary-ids-tree]
  (let [browser (.browse primary-name-tree)]
    (reduce (fn [r primary-name]
	      (if-let [ids (.find primary-ids-tree (key primary-name))]
		(assoc r (val primary-name) (distinct (cons (key primary-name) (val ids))))
		(assoc r (val primary-name) (list (key primary-name)))))
	    {}
	    (take-while identity (repeatedly (fn [] (let [t (Tuple.)]
						      (when (.getNext browser t)
							t))))))))
  

(defn- recman [db-struct day]
  (let [day-recmans (:day-record-managers db-struct)]
    (loop [retries 0]
      (if-let [recman (get @day-recmans day)]
	recman
	(let [new-recman (try
			   (let [r (RecordManagerFactory/createRecordManager (.getPath (File. (:path db-struct) (Integer/toString day))))
				 lockobj (Object.)]
			     
				 (locking lockobj
				   (dosync (alter (:lock db-struct) assoc r lockobj))
				   (let [primary-name-tree (let [k (.getNamedObject r "keys")]
							     (if (zero? k)
							       (let [bt (BTree/createInstance r)]
								 (.setNamedObject r "keys" (.getRecid bt))
								 bt)
							       (BTree/load recman k)))
					 primary-ids-tree (let [k (.getNamedObject r "keys")]
							    (if (zero? k)
							      (let [bt (BTree/createInstance r)]
								(.setNamedObject r "keys" (.getRecid bt))
								bt)
							      (BTree/load r k)))
					 name-ids    (read-name-ids r primary-name-tree primary-ids-tree)]
				     [r primary-name-tree primary-ids-tree name-ids])))
			     (catch IOException e e))]
	  (if (instance? Exception new-recman)
	    (if (< 100 retries)
	      (do
		(Thread/yield)
		(recur (inc retries)))
	      (throw new-recman))
	    (dosync (if-let [recman (get @day-recmans day)]
		      (throw (IllegalStateException. "Huh? Got some other recman while one was created by me"))
		      (do (alter day-recmans assoc day (first new-recman))
			  (alter (:recman-primary-name-trees db-struct) assoc (first new-recman) (second new-recman))
			  (alter (:recman-primary-ids-trees db-struct) assoc (first new-recman) (nth new-recman 2))
			  (alter (:date-name-ids db-struct) assoc day (nth new-recman 3))
			  (first new-recman))))))))))

(defn use-day [db-struct date]
  (let [day (date-as-day date)
	recman (recman db-struct day)
	day-users (:day-users db-struct)
	unused-days-lru (:unused-days-lru db-struct)] 
    (dosync
     (alter day-users (fn [du] (if-let [m (get du day)]
					     (assoc du day (inc m))
					     (assoc du day 1))))
     (alter unused-days-lru (fn [unused] (into [] (filter #(not= day %) unused))))) 
    recman))

(defn day-for-recman [db-struct recman]
  (some #(when (= (val %) recman) (key %)) @(:day-record-managers db-struct)))

(defn return-day [db-struct date]
  (let [unused-days-lru (:unused-days-lru db-struct)
	day-record-managers (:day-record-managers db-struct)
	day-users (:day-users db-struct)
	day (if (instance? RecordManager date)
	      (day-for-recman db-struct date)
	      (date-as-day date))
	to-close (atom nil)
	keep @(:days-to-keep db-struct)]
    (when day
      (dosync
       (alter day-users (fn [du] (if-let [users (get du day)]
				   (if (< users 2)
				     (do 
				       (alter unused-days-lru conj day)
				       (let [c (count @unused-days-lru)]
					 (when (< keep c)
					   (let [old-days (take (- c keep) @unused-days-lru)]
					     (swap! to-close (fn [to-close] (reduce (fn [r e]
										       (assoc r e (get @day-record-managers e)))
										    {} old-days)))
					     
					     (alter day-record-managers (fn [rm] (reduce (fn [r e]
											   (when-let [the-recman (get r e)]
											     (alter (:recman-primary-name-trees db-struct) dissoc the-recman)
											     (alter (:recman-primary-ids-trees db-struct) dissoc the-recman)
											     (alter (:lock db-struct) dissoc the-recman)
					;should we remove from date-name-ids? or keep it as cache for client listings?
											     )
											     
											   (dissoc r e))
											 rm
											 old-days)))
					     (alter unused-days-lru (fn [unused] (into [] (drop (- c keep)  unused)))))))
				       (dissoc du day))
				     (assoc du day (dec users)))
				   du))))
      
      (doseq [me @to-close]
	(try
	  (.close ^RecordManager (val me))
	  (catch IOException e
	    (println "Could not close" (key me) " " (.getMessage. e))))))))

(def *recman* nil)

(defmacro using-day [db-struct date & body]
  `(when-let [recman# (use-day ~db-struct ~date)]
     (binding [*recman* recman#]
       (try
	 (do ~@body)
	 (finally
	  (return-day ~db-struct ~date))))))

(deftest test-using-day
  (with-temp-dir
    (let [db (db-struct (.getPath *temp-dir*))]
      (println "Keeping 3 days")
      (reset! (:days-to-keep db) 3)
      (println "Adding 3 20110101")
      (dotimes [_ 3]
	(use-day db 20110101))
      (is (= 3  (val (first  @(:day-users db)))))
      (is (zero? (count @(:unused-days-lru db))))
      (println "Returning 2 days")
      (dotimes [_ 2]
	(return-day db 20110101))
      (is (= 1  (val (first  @(:day-users db)))))
      (is (zero? (count @(:unused-days-lru db))))
      (println "Returning last 20110101")
      (return-day db 20110101)
      (is (= 1 (count @(:unused-days-lru db))))
      (is (= 20110101 (first @(:unused-days-lru db))))
      (println "Adding 20110102 20110103 20110104 20110105")
      (use-day db 20110102)
      (use-day db 20110103)
      (use-day db 20110104)
      (use-day db 20110105)
      (is (= 4 (count  @(:day-users db))))
      (println "Returning 20110105 20110104")
      (return-day db 20110105)
      (return-day db 20110104)
      (is (= 20110101 (first @(:unused-days-lru db))))
      (is (= 2 (count  @(:day-users db))))
      (is (= 5 (count  @(:day-record-managers  db))))
      (println "Returning 20110103")
      (return-day db 20110103)
      (is (= 4 (count  @(:day-record-managers  db))))
      (is (not= 20110101 (first @(:unused-days-lru db))))
      (is (= 1 (count  @(:day-users db))))      
      (println "Returning 20110103 again")
      (return-day db 20110103)
      (is (= 4 (count  @(:day-record-managers  db))))
      (doseq [d-m @(:day-record-managers db)]
	(is (get @(:recman-primary-name-trees db) (val d-m)))
	(is (get @(:recman-primary-ids-trees db) (val d-m))))
      (is (= 3 (count @(:unused-days-lru db))))
      (is (= 5 (count  @(:date-name-ids db)))) ;Should  be 5 as long as we don't remove them up on close

      )))


(defn names-and-ids
  [db-struct day]
  (let [day (date-as-day day)]
    (if-let [names-primaries (get @(:date-name-ids db-struct) day)]
      names-primaries
      (using-day db-struct day
		 (get @(:date-name-ids db-struct) day)))))
  
(defn names [db-struct day]
  (keys (names-and-ids db-struct day)))


(defn days-with-data [])

(defmacro using-db-env [path & body])

(defn sync-database []) ;;is it used?

;May return a btree with another id, if this happens to be occupied by browsers
(defn for-writing [db-struct day id]
  (BTree/load *recman* id)
  ) 

(defn return-for-writing [db-struct btree])

(def *btree* nil)

(defmacro writing [db-struct day id & form]
  `(binding [*btree* (for-writing ~db-struct ~day ~id)]
     (try
       (do ~@form)
       (finally (return-for-writing db-struct *btree*)))))

(defn- create-new-data-id [db-struct time tname] ;This may overwrite any existing recid for this name!!!!
  (println "inserting for" tname)
  (using-day db-struct time
	     (let [day (date-as-day time)
		   btree (BTree/createInstance *recman*)
		   recid (.getRecid btree)
		   primary-name (get @(:recman-primary-name-trees db-struct) *recman*)]
	       (.insert primary-name recid tname false)
	       (dosync
		(alter (:date-name-ids db-struct) assoc-in [day tname] [recid]))
	       recid)))
		
		       


(defn add-to-db [db-struct value time keys]
					;Here we have to take care of the situation when someone is browsing the name and date provided!!!
					;We also have to make sure, no one start reading last while this is happening
  (let [day (date-as-day time)]
    (using-day db-struct day
	       (let [id (locking (get @(:lock db-struct) *recman*)
			  (if-let [current-ids (get (names-and-ids db-struct day) keys)]
			    (last current-ids)
			    (do
			      (create-new-data-id db-struct day keys))))]
		 (writing db-struct day id
			  (try
			    (println *btree* "id" (.getRecid *btree*))
			    (println time)
			    (println value)
			    (.insert *btree* (.getTime time) value true)
			    (catch IOException e
			      (println "Couldn't write" time value))))))))
  
(deftest add-some
  (with-temp-dir
    (let [date (Date.)
	  db (db-struct (.getPath *temp-dir*))
	  name {:snigel "Olja" :Balsam "Bröd"}]
      (println (:lock db))
      (using-day db date
		 (println (:lock db))
		 (println *recman*)
		 (println "Names before" (names db date)))
      (add-to-db db 32 date name)
      (using-day db date
		 (println "names" (names db date))
		 (println "Names and ids" (names-and-ids db date))
		 (println (get @(:recman-primary-ids-trees db) *recman*))
		 (let [
		       browser (.browse (BTree/load *recman* (first (get (names-and-ids db date) name))))
		       tuple (Tuple.)]
		   (is (.getNext browser tuple))
		   (println (Date. (.getKey tuple)) "=" (.getValue tuple)))
		 (doseq [v (apply browse db *recman* (get (names-and-ids db date) name))]
		   (println (Date. (first v)) "==" (second v)))
		 (println "Using browser")
		 (doseq [v (apply browser db *recman* (get (names-and-ids db date) name))]
		   (println (Date. (first v)) "==" (second v))))))) 
		  


(defn remove-date [date])

(defn compress-data
  ([date termination-fn])
  ([date] (compress-data date (fn [] false))))

(defn get-from-db [fun day keys-and-data])