; Släpp BTree om data definitivt inte finns snapshot
; Släpp lås 
; Do proper close of DB

(ns monitor.newdb
  (:import (java.util Date))
  (:import (java.util.concurrent TimeUnit CountDownLatch))
  (:import (java.util.concurrent.locks ReentrantLock))
  (:import (java.text SimpleDateFormat))
  (:import (java.io IOException File FilenameFilter RandomAccessFile FileFilter))
  (:import (java.nio.channels OverlappingFileLockException))
  (:import (jdbm RecordManagerFactory RecordManager))
  (:import (jdbm.btree BTree))
  (:import (jdbm.helper Tuple TupleBrowser))
  (:use clojure.test)
  (:use [clojure.pprint :only [pprint]])
  (:use monitor.tools)
  (:use (clojure.contrib profile)))

(comment
  En db per dag, för snabb delete, och manuell patchning, och förhindra defrag behov
  Varje dag har en recman
  Varje dag har "Keys" med recid som nyckel, och map av string-string. RecId är unik. Värden för samma data kan sparas i flera BTree. Vi kan uppdatera i en annan BTree när vi har en browser
  Data är BTree för ett namn, och har timestamp-value. Data har inte namn. 
  En Data kan skrivlåsas för att supporta TupleBrowser
  En dag tas bort när ingen har åtkomst. Ev file del, med stängd recman.
  Gamla recmans stängs automatiskt, för att inte ta upp cache ?
  )

(comment
  The last recid have to be locked for writing if it is browsed. A writer usually creates a new last recid, and can hence clear the all other browser locks in the sequence.
  The last recid have to be locked for browsing if it is written to. A browser usually waits until the write is complete until it tries to create a browser for the last recid.
  This means that we only care about locking the last recid. All browser locks are cleared when a new writer recid is created
  Browser will leave browse lock if not browsed to end. Does this mean that each client should be able to manually delete its locks- Cumbersome since browsing is made through seq. Perhaps a lock should contain a id to its user, Invalidating browsers when released, or when client leaves the database. Guess that a browser should be something more advanced, being a seqable that is also closeable, Either autoclosed on end, or able to follow writes in rt.  )

;For compatibility, this is set when using-db-env
(def *db-env* (atom nil)) ;I know this does qualify for ear mufs

(defn- read-name-ids
  "Returns a map of names and seq of ids using a record manager and belonging BTrees primaryid-name and primary-ids"
  [recman ^BTree primary-name-tree ^BTree primary-ids-tree]
  (let [browser (.browse primary-name-tree)]
    (reduce (fn [r primary-name]
	      (if-let [ids (.find primary-ids-tree (first primary-name))]
		(assoc r (second primary-name) (distinct (cons (first primary-name)  ids)))
		(assoc r (second primary-name) (list (first primary-name)))))
	    {}
	    (take-while identity (repeatedly (fn [] (let [t (Tuple.)]
						      (when (.getNext browser t)
							[(.getKey t) (.getValue t)]))))))))

(defn- new-day
  "Creates a new daystruct out of a path and day, reading meta data from db"
  [path day]
  ;wait if being deleted
  (let [r  (RecordManagerFactory/createRecordManager
	    (.getPath (File. path (Integer/toString day))))]

      (let [primary-name-tree (let [k (.getNamedObject r "keys")]
				(if (zero? k)
				  (let [bt (BTree/createInstance r)]
				    (.setNamedObject r "keys" (.getRecid bt))
				    bt)
				  (BTree/load r k))) ;if this is empty, this is a partially deleted
	    primary-ids-tree (let [k (.getNamedObject r "ids")]
			       (if (zero? k)
				 (let [bt (BTree/createInstance r)]
				   (.setNamedObject r "ids" (.getRecid bt))
				   bt)
				 (BTree/load r k)))
	    name-ids (read-name-ids r primary-name-tree primary-ids-tree)
	    browsers (ref {})
	    latches (ref {})
	    closed (atom false)
	    deleted (atom false)
            lock (java.util.concurrent.locks.ReentrantLock.)]
	
	{:name day
	 :recman r
         :uncommitted-inserts (atom 0)
	 :lock lock
	 :primary-name-tree primary-name-tree
	 :primary-ids-tree primary-ids-tree
	 :name-ids (ref name-ids)
	 :browser-recid browsers
	 :writter-recid-latches latches
	 :closed closed
	 :deleted deleted
	 :close (fn close-day []
                  (.lock lock)
                  (try
		  (reset! closed true) 
		  (doseq [br @browsers]
		    (println "Closing browser")
		    (.close  (key br)))
		  (doseq [la @latches]
		    (println "Waiting on writer to complete")
		    (if-not (wait-on-latch (val la) 2000 (fn [] false))
		      (println "Failed")
		      (println "OK")))
			  
		  (try
		    (.commit r)
		    (.close ^RecordManager r)
		    (catch IOException e
		      (println "Could not close" day " " (.getMessage e))))
		  (finally (.unlock lock))))})))


(def get-day-lock (Object.))
(defn- get-day 
  "Returns a day-struct for a day. Loading it if not found in memory"
  [db-struct day]
  (locking get-day-lock
  (let [old-dayname-days (:dayname-days db-struct)]
    (loop [retries 0]
      (or (get @old-dayname-days day)
	  (let [a-new-day (try
			    (let [r (new-day (:path db-struct) day)]
                              (dosync (alter (:existing-days db-struct) conj day))
                              r)
			    (catch IOException e e))]
	    (if (instance? Exception a-new-day)
	      (if (< 100 retries)
		(do
		  (Thread/yield)
		  (recur (inc retries)))
		(throw a-new-day))
	      (dosync (if (get @old-dayname-days day)
			(throw (IllegalStateException. "Huh? Got some other day while one was created by me"))
			(do (alter old-dayname-days assoc day a-new-day)
			    a-new-day)))))))))  )

(defn use-day
  "Return a day-struct, and registering it as being in use, preventing it from being closed"
  ([db-struct date]
  (let [day (date-as-day date)
	day-struct (get-day db-struct day)
	day-users (:day-users db-struct)
	unused-days-lru (:unused-days-lru db-struct)] 
    (dosync
     (alter day-users (fn [du] (if-let [m (get du day)]
					     (assoc du day (inc m))
					     (assoc du day 1))))
     (alter unused-days-lru (fn [unused] (into [] (filter #(not= day %) unused))))) 
    day-struct))
  ([db-struct date dont-create]
     (if (and dont-create (not (contains? @(:existing-days db-struct) (date-as-day date))))
       nil
       (use-day db-struct date))))
         


(defn return-day
  "Realse useage of a day. Earlier used days will be closed, when enough amount has been used"
  [db-struct date]
  (let [unused-days-lru (:unused-days-lru db-struct)
	dayname-days  (:dayname-days db-struct)
	day-users (:day-users db-struct)
	day (or (:name date) (date-as-day date))		     	  
	to-close (atom nil)
	keep (:days-to-keep db-struct)] 
    (when day
      (dosync
       (alter day-users (fn [du] (if-let [users (get du day)]
				   (if (< users 2)
				     (do 
				       (alter unused-days-lru conj day)
				       (let [c (count @unused-days-lru)]
					 (when (< keep c)
					   (let [old-days (take (- c keep) @unused-days-lru)]
					     (swap! to-close (fn [to-close]
							       (reduce (fn [r e]
									 (assoc r e (get @dayname-days e)))
								       {} old-days)))

					     ;This statement should prob be removed. Closing stuff is performed in close ops further down
					     (alter dayname-days (fn [rm] (reduce (fn [r e]
										    (dissoc r e))
										  rm
										  old-days)))
					     (alter unused-days-lru (fn [unused] (into [] (drop (- c keep)  unused)))))))
				       (dissoc du day))
				     (assoc du day (dec users)))
				   du))))

      (doseq [me @to-close]
	((:close (val me)))))))




	;***********************************************************************
					; Browsing

(defn- prevent-conc-writes
  "Prevent concurrent concurrent writes of btrees identified by ids. Will wait up to 10 s if there already is a writer. Writing may be aborted using supplied fn terminate?. Concurrent writes will be directed to a new btree. The browser will have a snapshot view" 
 [day-struct browser terminate? ids ]
     (doseq [id ids]
       (when-let [latch (dosync (alter (:browser-recid day-struct) assoc browser id)
				(get @(:writter-recid-latches day-struct) id))]
	 (when-not (wait-on-latch latch 10000 terminate?)
	   (throw (RuntimeException. "A concurrent write took too long"))))))

(defn- allow-writes
  "Allow concurrent writes from a browsers perspective"
  [day-struct browser]
    (dosync (alter (:browser-recid day-struct) dissoc browser)))

(defn tuple-browser-seq
  "Returns a Lazy seq of browsing using a tuple browser"
  [^TupleBrowser tuple-browser day-struct]
  (let [f (fn f [^Tuple prev]
            (let [t (Tuple.)]
              (let [status (try (.getNext tuple-browser t))] 
                  (if status
                    (lazy-seq (cons [(.getKey prev) (.getValue prev)] (f t)))
                    (list [(.getKey prev) (.getValue prev)])))))
	t (Tuple.)]
    (when (.getNext tuple-browser t)
      (f t))))
	
  
(defn- browse
  "Interleaved browsing using a set of btrees, identified by ids. Elements turn up in key order, independent of BTree."
  [day-struct browser ids] 
   (let [seqs (map (fn [id] (tuple-browser-seq (.browse (BTree/load (:recman day-struct) id)) day-struct)) ids)
	compare-first (fn [a b] (compare (first a) (first b)))]
     (map (fn [e] [(Date. ^Long (first e)) (second e)]) (apply interleave-sorted compare-first seqs))))

;"A Browser id seqable and closeable. Browsing is aborted when closed"
(deftype Browser 
  [day-struct close-fn ids]
  java.io.Closeable (close [this]
			   (close-fn this))
  clojure.lang.Seqable (seq [this]
			    (browse day-struct this ids)))


(defn browser [db-struct date keys]
  
  (let [date (date-as-day date)
        day (use-day db-struct date true)]
    (when day
      (let [name-and-ids (fn [d] @(:name-ids d))
            ids (get (name-and-ids day) keys)
            b  (Browser. day #(do (allow-writes day %1) (return-day db-struct date)) ids)]
        (prevent-conc-writes day b (fn [] false) ids)
        b))))

					;************************************************************
					;DAY HANDLING

	



(def *day* nil)

;  "Using a day in thread localbinding *day*"
(defmacro using-day

  [db-struct date & body]
  `(when-let [day# (use-day ~db-struct ~date)]
         (binding [*day* day#]
       (try
	 (do ~@body)
	 (finally
	  (return-day ~db-struct ~date))))))

(defn- names-and-ids
  "Returns names and ids, loading it temporarilly if not already in memory"
  [db-struct day]
  (let [day (date-as-day day)
	nids (fn [d] @(:name-ids d))]
    (if-let [day-struct (get @(:dayname-days db-struct) day)]
      (nids day-struct)
      (if-let [d (use-day db-struct day true)]
        (try
          (nids d)
          (finally (return-day db-struct day)))
        {}))))

(defn- create-new-data-id
  "Create a new primare id for a name and day"
  [db-struct day tname] ;This may overwrite any existing recid for this name!!!!
  (using-day db-struct day
	     (let [btree (BTree/createInstance (:recman *day*))
		   recid (.getRecid btree)
		   primary-name (:primary-name-tree *day*)
		   name-ids (:name-ids *day*)]
	       (.insert ^BTree primary-name recid tname false)
	       (dosync
		(alter name-ids assoc tname [recid]))
	       recid)))

;May return a btree with another id, if this happens to be occupied by browsers
(defn for-writing
  ([day-struct name-keys id time-left]
;     (.lock (:lock day-struct))
;     (try
  (let [has-reader (fn [id] (some #(= id %) (vals @(:browser-recid day-struct))))
	write-latch (fn [id] (get @(:writter-recid-latches day-struct) id))]
    (let [status (dosync (let [l (write-latch id)
			       r (has-reader id)
			       status (if @(:closed day-struct)
					:closed
					(if-not r
					  (if l
					    l
					    :ok)
					  :reader))]
			   (when (= :ok status)
			     (alter (:writter-recid-latches day-struct) assoc id (CountDownLatch. 1)))
			   status))]
      (let [timestamp (System/currentTimeMillis)]
	(when-not  (= :closed status)     
	  (if (= :ok status)
	    (BTree/load (:recman day-struct) id)
	    (if (= :reader status)
	      (let [new-tree (BTree/createInstance (:recman day-struct))]
		(let [id-strip (dosync
				(let [recid (.getRecid new-tree)
				      new-id-strip (conj (get @(:name-ids day-struct) name-keys) recid)]
				  (alter (:writter-recid-latches day-struct) assoc recid (CountDownLatch. 1))
				  (alter (:name-ids day-struct) (fn [name-ids] (assoc name-ids name-keys new-id-strip)))
				  new-id-strip))]
		  (.insert ^BTree (:primary-ids-tree day-struct) (first id-strip) (next id-strip) true))
		new-tree)
	      (if (.await ^CountDownLatch status time-left TimeUnit/MILLISECONDS)
		(recur day-struct name-keys id (- time-left (- (System/currentTimeMillis) timestamp)))
		(throw (RuntimeException. "Could not write in time")))))))))
  ;(finally (.unlock (:lock day-struct)))
  )
  ([day-struct name-keys id]
     (for-writing day-struct name-keys id 10000)))
      
	      


(defn return-for-writing [day-struct id]
  (dosync (alter (:writter-recid-latches day-struct) (fn [latch-table]
						       (when-let [latch ^CountDownLatch (get latch-table id)]
							 (.countDown latch))
						       (dissoc latch-table id)))))

(def *btree* nil)

(defmacro writing [ day-struct key-name id & form]
  `(binding [*btree* (for-writing ~day-struct ~key-name ~id)]
     (try
       (do ~@form)
       (finally (return-for-writing ~day-struct (.getRecid ^BTree *btree*))))))


(defn delete-day [db-struct day]
  (let [name (date-as-day day)]
    (using-day db-struct name
    (when (dosync
	   (if (and
		(= 1 (get @(:day-users db-struct) name)) 
		(empty? @(:browser-recid *day*))
		(empty? @(:writter-recid-latches *day*))
		(not (contains? @(:being-deleted db-struct) name)))
	     (do (reset! (:closed *day*) true)
		 (alter (:being-deleted db-struct) conj name)
		 (alter (:unused-days-lru db-struct) (fn [u] into [] (filter #(= name %) u)))
		 (alter (:dayname-days db-struct) dissoc name)
		 (alter (:day-users db-struct) dissoc name)
		 true)
	     false))
      (.lock (:lock *day*))
      (try
	((:close *day*)) ;should be close name on db-struct
	(let [files-to-delete (.listFiles (File. (:path db-struct))
					  (proxy [FilenameFilter] []
					    (accept [dir file-name]
						    (.startsWith ^String file-name (Integer/toString name)))))]
	  (doseq [file files-to-delete]
	    (let [rafile (RandomAccessFile. file "rw")
		  channel (.getChannel rafile)]
	      (try
	      (if-let [l (.tryLock channel)]
		(.release l)
		(throw (IOException. (.getPath file) "is in use" )))
	      (catch OverlappingFileLockException _
		(throw (IOException. (.getPath file) "is in use")))
	      (finally
	       (.close rafile)))))
          (dosync (alter (:existing-days db-struct) disj name)) 
	  (doseq [^File file files-to-delete]
	    (when-not (.delete file)
	      (throw (IOException. "Could not delete" (.getPath file))))))
	
	#_(let [recman (:recman day-struct)
		name-t (BTree/load recman  (.getNamedObject recman "keys"))
		ids-t (BTree/load recman (.getNamedObject recman "ids"))
		name-ids (read-name-ids recman name-t ids-t)]
	    (doseq [ids (vals name-ids)]
	      (doseq [id ids]
		(.delete (BTree/load recman id))))
	    (.delete name-t)
	    (.delete ids-t))
	(finally (dosync (alter (:being-deleted db-struct) disj name))
                 (.unlock (:lock *day*))))))))





					;*********************************************************************************
					;User interface

(defn db-struct [path unused-days-to-keep-open]
  (let [dayname-days (ref {})
	unused-days-lru (ref [])
	day-users (ref {})
	being-deleted (ref #{})
        existing-days (ref (reduce (fn [r e]
                                     (if-let [m  (re-matches #"^(\d{8})\." (.getName e))]
                                              (conj r (Integer/valueOf (second m)))
                                              r)
                                     ) #{} (.listFiles (File. path) (proxy [FileFilter] []
                                                                             (accept [^File file]
                                                                               (not (.isDirectory file)))))))
                                                                               ]
    
    {:path path
     :existing-days existing-days
     :days-to-keep unused-days-to-keep-open 
     :dayname-days dayname-days ;the open day structures according to day as int 
     :unused-days-lru unused-days-lru
                                        ;number of current users for each day-int
     :day-users day-users
     :being-deleted being-deleted
     :close (fn close-db []
	    (let [dn-d (dosync
			(let [r @dayname-days]
			  (ref-set dayname-days {})
			  (ref-set unused-days-lru [])
			  (ref-set day-users {})
			  r))]
	      (doseq [d dn-d]
		((:close (val d))))))}))

(def *db* nil)

(defmacro using-db
  "Use *db* thread locally bound to a temporary db-struct"
  [path unused-days-to-keep-open & body]
  `(when-let [db# (db-struct ~path ~unused-days-to-keep-open)]
     (binding [*db* db#]
       (try (do ~@body)
	    (finally ((:close db#)))))))

(defn days-with-data
  ([db-struct]
     @(:existing-days db-struct))
  ([]
     (if-let [env @*db-env*]
       (days-with-data env))))
  

(defmacro using-db-env [path & body]
  `(using-db ~path 3
             (reset! *db-env* *db*)
             (try
               (do ~@body)
               (finally (reset! *db-env* nil)))))

(defn names
  ([db-struct day]
     (keys (names-and-ids db-struct day)))
  ([day]
     (if-let [env @*db-env*]
       (names env day))))

(defn add-to-db
  ([db-struct value ^Date time keys]
     (let [day (date-as-day time)]
       (using-day db-struct day
                  	(let [last-id (fn [] (let [current-ids (get @(:name-ids *day*) keys)]
			  (if (seq current-ids)
			    (last current-ids))))]

               ;(.lock (:lock *day*))
               ;(try
	       (let [id (or (last-id)
			    (let [l ^ReentrantLock (:lock *day*)] 
			      (.lock l)
			      (try
				(or (last-id)
				    (create-new-data-id db-struct day keys))
				(finally (.unlock l)))))]
		 (writing *day* keys id
			  (try
			    (.insert ^BTree *btree* (.getTime time) value true)
                            (when (< 999 (swap! (:uncommitted-inserts *day*) inc))
                              (.commit (:recman *day*))
                              (reset! (:uncommitted-inserts *day*) 0))
			    (catch IOException e
			      (println "Couldn't write" time value)))))
               ;(finally (.unlock (:lock *day*)))
               ))))
  ([value time keys]
      (if-let [env @*db-env*]
       ( add-to-db env value time keys)
       ( add-to-db *db* value time keys))))

(defn sync-database
  ([db-struct]
  (dorun (for [d-s (vals @(:dayname-days db-struct))]
           (do ;(println "commit")
             ;(.lock (:lock d-s))
             ;(try
               (.commit (:recman d-s))
               (reset! (:uncommitted-inserts d-s) 0)
             ;  (finally (.unlock (:lock d-s))))
             ))))
  ([]
     (if-let [env @*db-env*]
       (sync-database env)
       (sync-database *db*))))
     

(defn remove-date
  ([db-struct date]
     (if-let [env @*db-env*]
       (remove-date env date)
       (remove-date *db* date))))

(defn compress-data
  ([date termination-fn])
  ([date] (compress-data date (fn [] false))))

(defn get-from-db [fun day keys-and-data]
  (when-let [db @*db-env*]
    (with-open [b ^Browser (browser db day keys-and-data)]
      (doseq [e (seq b)]
        (fun [e])))))

(defn tree-ids-of [db-struct date keys]
  (get (names-and-ids db-struct date) keys))



(deftest test-using-day
  (with-temp-dir
    (let [db (db-struct (.getPath *temp-dir*) 3)]
      (println "Keeping 3 days")
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
;      (println "day-users" @(:day-users db))
;      (println "unused-days"  @(:unused-days-lru db))
;      (println "dayname-days" @(:dayname-days db)) 
      (println "Returning last 20110101")
      (return-day db 20110101)
            ;(println "day-users" @(:day-users db))
;      (println "unused-days"  @(:unused-days-lru db))
;      (println "dayname-days" @(:dayname-days db)) 

      (is (= 1 (count @(:unused-days-lru db))))
      (is (= 20110101 (first @(:unused-days-lru db))))
      (println "Ading 20110102 20110103 20110104 20110105")
      (use-day db 20110102)
      (use-day db 20110103)
      (use-day db 20110104)
      (use-day db 20110105)
;            (println "day-users" @(:day-users db))
;      (println "unused-days"  @(:unused-days-lru db))
;      (println "dayname-days" @(:dayname-days db)) 

      (is (= 4 (count  @(:day-users db))))
      (println "Returning 20110105 20110104")
      (return-day db 20110105)
      (return-day db 20110104)
;            (println "day-users" @(:day-users db))
;      (println "unused-days"  @(:unused-days-lru db))
;      (println "dayname-days" @(:dayname-days db)) 

      (is (= 20110101 (first @(:unused-days-lru db))))
      (is (= 2 (count  @(:day-users db))))
      (is (= 5 (count  @(:dayname-days  db))))
      (println "Returning 20110103")
      (return-day db 20110103)
      (is (= 4 (count  @(:dayname-days  db))))
      (is (not= 20110101 (first @(:unused-days-lru db))))
      (is (= 1 (count  @(:day-users db))))      
      (println "Returning 20110103 again")
      (return-day db 20110103)
      (is (= 4 (count  @(:dayname-days db))))
      (doseq [d-m @(:dayname-days db)]
	(is (:primary-name-tree (val d-m)))
	(is (:primary-ids-tree (val d-m))))
      (is (= 3 (count @(:unused-days-lru db))))
      )))

(deftest add-some
  (with-temp-dir

    (println "The temp path is " (.getPath *temp-dir*))
    (let [
	  date (Date.)
	  date2 (Date. (+ 2000 (.getTime date)))
	  db (db-struct (.getPath *temp-dir*) 100)
	  name {:snigel "Olja" :Balsam "Bröd"}]
      (using-day db date
		 (add-to-db db 32 date name)
		 (add-to-db db 37 date2 name))
      (using-day db date
		 (let [browser (.browse (BTree/load (:recman *day*) (first (get (names-and-ids db date) name))))
		       tuple (Tuple.)]
		   (is (.getNext browser tuple))
		   (is (= 32 (.getValue tuple)))
		   (is (= date (Date.(.getKey tuple))))
;		   (println (Date. (.getKey tuple)) "=" (.getValue tuple))
		   (is (.getNext browser tuple))
		   (is (not (.getNext browser tuple))))
		 
		 
		 ;(doseq [v (apply browse db *day* (get (names-and-ids db date) name))]
		  ; (println (Date. (first v)) "==" (second v)))
		 (println "Using browser")
		 (with-open [b (browser db date name)]
		   (doseq [v b];(apply browser *day* (get (names-and-ids db date) name))]
					;(println (Date. (first v)) "==" (second v)))
		     (println (first v) "==" (second v))))
		 (println @(:name-ids *day*))
		 (doseq [t (tuple-browser-seq (.browse (BTree/load (:recman *day*) (first (val (first @(:name-ids *day*)))))))]
		   (println t))
		 (doseq [t (apply interleave-sorted #(compare (first %1) (second %1)) [(tuple-browser-seq (.browse (BTree/load (:recman *day*) (first (val (first @(:name-ids *day*)))))))])]
		   (println t))
		 (is (= 1 (count @(:name-ids *day*))))
		 (is (= 1 (count (val (first  @(:name-ids *day*))))))
		 (is (empty? @(:browser-recid *day*)))
		 (is (empty? @(:writter-recid-latches *day*)))

		 (with-open [b (browser db date name)];(apply browser *day* (get (names-and-ids db date) name))]
		   (is (= 1 (count @(:browser-recid *day*))))
		   (is (= (get @(:browser-recid *day*) b) (first (val (first  @(:name-ids *day*))))))
		   (.close b)
		   (is (= 0 (count @(:browser-recid *day*)))))
		 (with-open [b (browser db date name)];(apply browser *day* (get (names-and-ids db date) name))
		      (let [first-id ( first (val (first  @(:name-ids *day*))))]
			(add-to-db db 42 (Date. (+ 3000 (.getTime date))) name)
			(is (= 1 (count @(:browser-recid *day*))))
			(is (= 2 (count (val (first  @(:name-ids *day*))))))
			(is (= first-id (first (val (first  @(:name-ids *day*))))))
			(is (= (map #(second %) b) [32 37]))
			(is (= (map #(second %) (browser db date name));(apply browser *day* (get (names-and-ids db date) name))
			        [32 37 42])) )
		      )))))



(deftest speed
  (with-temp-dir
    (let [df (SimpleDateFormat. "yyyyMMdd HHmmss")
	  dates (iterate #(Date. (+ 1000 (.getTime %))) (.parse df "20110101 000000" ))
	  db (db-struct (.getPath *temp-dir*) 100)
	  a-name {:olja "besin"}]

      (using-day db (first dates)
		 (add-to-db db 42 (first dates) a-name)
		 (println "insert")
		 (time (doseq [d (take 80000 dates)]
			 (add-to-db db 42 d a-name)))
		 (println "Commit")
		 (time (.commit (:recman *day*)))
		 (.clearCache (:recman *day*))
		 (println "size:" (files-size *temp-dir*))
		 (println "Read")
		 (with-open [b (browser db 20110101 a-name)]
		   (println (time  (count (seq b)))))
;		 (time (doseq [id (get @(:name-ids *day*) a-name)]
					;			 (.delete (:recman *day*) id)))
		 
		 (time (.commit (:recman *day*)))
		 
		 (delete-day db 20110101)
		 ;(.delete (:recman *day*) 0)
	;	 (.commit (:recman *day*))
	;	 (.defrag (:recman *day*))
		 ((:close db))
		 (println "size:" (files-size *temp-dir*))
		 (doseq [f (.list *temp-dir*)]
		   (println f)))
		 
      #_(let [dates (iterate #(Date. (+ 1000 (.getTime %))) (.parse df "20110102 000000" ))]
	(using-day db (first dates)
		   (add-to-db db 42 (first dates) a-name)
		   (time (doseq [d (take 5000 dates)]
			   (add-to-db db 42 d a-name)))
		   (time (.commit (:recman *day*)))
		   (println "-----")
		   (println @(:name-ids *day*))
		   (let [id (first (val (first @(:name-ids *day*))))
		      
			 tps (tuple-browser-seq (.browse (BTree/load (:recman *day*) id)))]
		     (println id)
		     (time (last tps)))))
		     
	
      )))

(defn do-a-browse [db-struct day name]
  (using-day db-struct day
	     (with-open [b (browser *day* name)]
	       (count (seq b)))))

(defn testsome []
  (println "ENTER!")
  (read)
  (with-temp-dir
   (using-db (.getPath *temp-dir*) 3
  (let [df (SimpleDateFormat. "yyyyMMdd HHmmss")
	dates (iterate #(Date. (+ (* 15 1000) (.getTime %))) (.parse df "20110101 000000" ))
	stop (atom false)
	part-name  "Banankontakt av tredje graden. Banan kontakter springer runt-"]
    (let [db *db*
	  fe (future (while (not @stop)
		    (Thread/sleep 1000)
		    (println (do-a-browse db 20110101 {:name (str part-name 1)}) "records found for a name")))]
      (time (dorun
	     (for [d (take (* 24 60 4) (seq dates))]
	       (do
					;(println d)
		 (dorun (for [i (range 100)]
			  (do
			    (add-to-db *db* i d {:name (str part-name i)}))))
		 (sync-database *db*)))))
    
      (reset! stop true)
      @fe)
    (println (do-a-browse *db* 20110101 {:name (str part-name 1)}) "records afterwards")
    (println "sixe=" (files-size *temp-dir*))
    (println "trees:" (count (tree-ids-of *db* 20110101 {:name (str part-name 1)})))
    (println "trees:" (count (tree-ids-of *db* 20110101 {:name (str part-name 2)})))))))


#_(deftest rup
  (let [df (SimpleDateFormat. "yyyyMMdd HHmmss")
	dates (iterate #(Date. (+ 1000 (.getTime %))) (.parse df "20110101 000000" ))]
    (println "ööö")
    (time (last (take 1000000 dates)))

    (time (last (interleave-sorted #(compare (.getTime ^Date %1) (.getTime ^Date %2))  (take 1000000 dates))))      
    
    (time (last (take 1000000 dates)))
    (time (last (interleave-sorted #(compare (.getTime %1) (.getTime %2))  (take 1000000 dates))))
          (time (last (interleave-sorted #(compare (.getTime ^Date %1) (.getTime ^Date %2))  (take 1000000 dates))))      
      ))



