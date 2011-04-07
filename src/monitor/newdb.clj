;       Fixa låsnigns strategi!!


(ns monitor.newdb
  (:import (java.util Date))
    (:import (java.util.concurrent TimeUnit CountDownLatch))
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

(defn compare-first [a b] (compare (first a) (first b)))
(defn interleave-sorted [compare-fn & ss]
  (when-let [seqs (into #{} (filter identity (map #(seq %) ss)))]
    (let [comp-fn (fn [a b] (compare-fn (first a) (first b)))
	  f (fn f [seqs]
	      (let [with-smallest (second (first (sort comp-fn (map (fn [e] [(first e) e]) seqs))))
		    smallest-value (first with-smallest)
		    remaining-seqs (if-let [rem (next with-smallest)]
				     (conj (disj seqs with-smallest) rem)
				     (disj seqs with-smallest))]
		(if (seq remaining-seqs)
		  (lazy-seq (cons smallest-value (f remaining-seqs)))
		  (list smallest-value))
	       ))]
    (f seqs))))

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

(defn- new-day [path day]
  
  (let [nome (.getPath (File. path (Integer/toString day)))
	r  (RecordManagerFactory/createRecordManager nome)
	lockobj (java.util.concurrent.locks.ReentrantLock.)]
    (.lock lockobj)
    (try
      (let [primary-name-tree (let [k (.getNamedObject r "keys")]
				(if (zero? k)
				  (let [bt (BTree/createInstance r)]
				    (.setNamedObject r "keys" (.getRecid bt))
				    bt)
				  (BTree/load r k)))
	    primary-ids-tree (let [k (.getNamedObject r "ids")]
			       (if (zero? k)
				 (let [bt (BTree/createInstance r)]
				   (.setNamedObject r "ids" (.getRecid bt))
				   bt)
				 (BTree/load r k)))
	    name-ids (read-name-ids r primary-name-tree primary-ids-tree)]
	{:recman r
	 :lock lockobj
	 :primary-name-tree primary-name-tree
	 :primary-ids-tree primary-ids-tree
	 :name-ids (ref name-ids)
	 :browser-recid (ref {})
	 :writter-recid-latches (ref {})})
      (finally (.unlock lockobj)))))

(defn db-struct [path]
  {:path path ;The directory where db is
   :days-to-keep (atom 10)
   :dayname-days (ref {})
   ;:day-record-managers (ref {}) ;day-int and record-manager
   :unused-days-lru (ref []) ; unused day-ints lru, head will be removed. **Test that this is ok**
   :day-users (ref {}) ;number of current users for each day-int
   ;:recman-primary-name-trees (ref {}) ;Btrees to update and initially read recid-names. One for each recman
   ;:recman-primary-ids-trees (ref {}) ;Btrees to update and initially read recids for a primary recid. One for each recman.
   ;:date-name-ids (ref {}) ; maps of name and recids (primary and following) for different dates.  
   ;:lock (ref {}) ;lock per recman, for sensitive things. Guess we should use e.g. Reentrant 
   ;:browsed-ids-per-recid (ref {}) ;number of browsers per recid for each recman. A writer should create a new id if current is browsed. The recid is cleared when ever a new writer recid is created. That is, its ok if a browser never reads to end.
   ;:written-ids-per-recid (ref {}) ;written recid per recman, A writer to a recman, should hold a lock, wich the browser can wait on
   })

(comment
  The last recid have to be locked for writing if it is browsed. A writer usually creates a new last recid, and can hence clear the all other browser locks in the sequence.
  The last recid have to be locked for browsing if it is written to. A browser usually waits until the write is complete until it tries to create a browser for the last recid.
  This means that we only care about locking the last recid. All browser locks are cleared when a new writer recid is created
  Browser will leave browse lock if not browsed to end. Does this mean that each client should be able to manually delete its locks- Cumbersome since browsing is made through seq. Perhaps a lock should contain a id to its user, Invalidating browsers when released, or when client leaves the database. Guess that a browser should be something more advanced, being a seqable that is also closeable, Either autoclosed on end, or able to follow writes in rt.  )
  
(defn- prevent-conc-writes [day-struct browser id]
    (let [latch (dosync (alter (:browser-recid day-struct) assoc browser id)
		      (get @(:writter-recid-latches day-struct) id))]
    (when latch
      (if-not (.await latch 10 TimeUnit/SECONDS)
	(throw (RuntimeException. "A concurrent write took too long"))))
					;What about interruption!!!
    )
  )

;Make sure Browser calls this on close
(defn- allow-writes [day-struct browser]
  (dosync (alter (:browser-recid day-struct) dissoc browser)))

(defn tuple-browser-seq [tuple-browser]
  (let [f (fn f [prev]
	    (let [t (Tuple.)]
	      (if (.getNext tuple-browser t)
		(lazy-seq (cons [(.getKey prev) (.getValue prev)] (f t)))
		(list [(.getKey prev) (.getValue prev)]))))
	t (Tuple.)]
    (when (.getNext tuple-browser t)
      (f t))))
	
  

(defn- browse [day-struct browser ids]
  (let [seqs (map (fn [id] (tuple-browser-seq (.browse (BTree/load (:recman day-struct) id)))) ids)]
    (apply interleave-sorted compare-first seqs)))

#_(defn- browse
  [day-struct browser ids]
  (let [f (fn f [jdbm-browser ids]
	    (let [tuple (Tuple.)]
	      (if (.getNext jdbm-browser tuple)
		(lazy-seq (cons [(.getKey tuple) (.getValue tuple)] (f jdbm-browser ids)))
		(do
		  (if-let [nnid (first ids)]
		    (do
		      (prevent-conc-writes day-struct browser nnid)
		      (let [browser (.browse (BTree/load (:recman day-struct) nnid))]
			(recur browser (next ids))))
		    (do
		      (allow-writes day-struct browser)
		      nil))))))]
    (when-let [nid (first ids)]
      (prevent-conc-writes day-struct browser nid)
      (try
	(f (.browse (BTree/load (:recman day-struct) nid)) (next ids))
	(catch Exception e
	  (allow-writes day-struct browser)
	  (throw e))))))



	
 
(defn- get-day [db-struct day]
  (let [old-dayname-days (:dayname-days db-struct)]
    (loop [retries 0]
      (if-let [old-day (get @old-dayname-days day)]
	old-day
	(let [a-new-day (try
			  (new-day (:path db-struct) day)
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
			  a-new-day)))))))))

(defn use-day [db-struct date]
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

(defn dayname-for-day [db-struct day-struct]
  (some #(when (= (val %) day-struct) (key %)) @(:dayname-days db-struct)))

(defn return-day [db-struct date]
  (let [unused-days-lru (:unused-days-lru db-struct)
	dayname-days  (:dayname-days db-struct)
	day-users (:day-users db-struct)
	day (if (:recman date)  ; Hmm, guess that a day could know wich day he is
	      (dayname-for-day db-struct date)
	      (date-as-day date))
	to-close (atom nil)
	keep @(:days-to-keep db-struct)]  ;days to keep should be provided as dbstruct arg
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
											   (when-let [the-recman (get r e)]
											 ;    (alter (:recman-primary-name-trees db-struct) dissoc the-recman)
											  ;   (alter (:recman-primary-ids-trees db-struct) dissoc the-recman)
											  ;   (alter (:lock db-struct) dissoc the-recman)
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
	;We should prob use close op on the day itself
	(try
	  (.close ^RecordManager (:recman (val me)))
	  (catch IOException e
	    (println "Could not close" (key me) " " (.getMessage. e))))))))

(def *day* nil)

(defmacro using-day [db-struct date & body]
  `(when-let [day# (use-day ~db-struct ~date)]
     (binding [*day* day#]
       (try
	 (do ~@body)
	 (finally
	  (return-day ~db-struct ~date))))))




(defn names-and-ids
  [db-struct day]
  (let [day (date-as-day day)
	name-and-ids (fn [d] @(:name-ids d))]
    (if-let [day-struct (get @(:dayname-days db-struct) day)]
      (name-and-ids day-struct)
      (using-day db-struct day
		 (name-and-ids *day*)))))

  
(defn names [db-struct day]
  (keys (names-and-ids db-struct day)))

;This fellow should take care of returning the day struct when browsing ends. It should create the day-struct, and take a time as arg
(deftype Browser [day-struct ids]
  java.io.Closeable
  (close [this]
	 (allow-writes day-struct this))
  clojure.lang.Seqable
  (seq [this]
       (browse day-struct this ids)))

 
(defn- browser-for-ids [day-struct ids]
  (let [b  (Browser. day-struct ids)]
    (doseq [id ids]
      (prevent-conc-writes day-struct b id))
    b))
    
 

(defn browser [day-struct keys]
  (let [name-and-ids (fn [d] @(:name-ids d))
	ids (get (name-and-ids day-struct) keys)]

    (browser-for-ids day-struct ids)))

(defn days-with-data [])

(defmacro using-db-env [path & body])

(defn sync-database []) ;;is it used?

;May return a btree with another id, if this happens to be occupied by browsers
(defn for-writing
  ([day-struct name-keys id time-left]
  (let [has-reader (fn [id] (some #(= id %) (vals @(:browser-recid day-struct))))
	write-latch (fn [id] (get @(:writter-recid-latches day-struct) id))]
    (let [status (dosync (let [l (write-latch id)
			       r (has-reader id)
			       status (if-not r
					(if l
					  l
					  :ok)
					:reader)]
			   (when (= :ok status)
			     (alter (:writter-recid-latches day-struct) assoc id (CountDownLatch. 1)))
			   status))]
      (let [timestamp (System/currentTimeMillis)]
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
		(.insert (:primary-ids-tree day-struct) (first id-strip) (next id-strip) true))
	      new-tree)
	    (if (.await status time-left TimeUnit/MILLISECONDS)
	      (recur day-struct name-keys id (- time-left (- (System/currentTimeMillis) timestamp)))
	      (throw (RuntimeException. "Could not write in time")))))))))
  ([day-struct name-keys id]
     (for-writing day-struct name-keys id 10000)))
      
	      


(defn return-for-writing [day-struct id]
  (dosync (alter (:writter-recid-latches day-struct) (fn [latch-table]
						       (when-let [latch (get latch-table id)]
							 (.countDown latch))
						       (dissoc latch-table id)))))

(def *btree* nil)

(defmacro writing [ day-struct key-name id & form]
  `(binding [*btree* (for-writing ~day-struct ~key-name ~id)]
     (try
       (do ~@form)
       (finally (return-for-writing ~day-struct (.getRecid *btree*))))))

;Part of this should be a member of day
(defn- create-new-data-id [db-struct time tname] ;This may overwrite any existing recid for this name!!!!
  (using-day db-struct time
	     (let [day (date-as-day time)
		   btree (BTree/createInstance (:recman *day*))
		   recid (.getRecid btree)
		   primary-name (:primary-name-tree *day*)
		   name-ids (:name-ids *day*)]
	       (.insert primary-name recid tname false)
	       (dosync
		(alter name-ids assoc tname [recid]))
	       recid)))
		
		       


(defn add-to-db [db-struct value time keys]
					;Here we have to take care of the situation when someone is browsing the name and date provided!!!
					;We also have to make sure, no one start reading last while this is happening
  (let [day (date-as-day time)
	last-id (fn [] (let [current-ids (get @(:name-ids *day*) keys)]
			  (if (seq current-ids)
			    (last current-ids))))]
    (using-day db-struct day
	       (let [id (if-let [id (last-id)]
			  id
			  (do
			    (.lock (:lock *day*))
			    (try
			      (if-let [id (last-id)]
				id
				(create-new-data-id db-struct day keys))
			      (finally (.unlock (:lock *day*))))))]
		 (writing *day* keys id
			  (try
			    (.insert *btree* (.getTime time) value true)
			    (catch IOException e
			      (println "Couldn't write" time value))))))))



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
      (println "Adding 20110102 20110103 20110104 20110105")
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
	  db (db-struct (.getPath *temp-dir*))
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
		 (with-open [b (browser *day* name)]
		   (doseq [v b];(apply browser *day* (get (names-and-ids db date) name))]
					;(println (Date. (first v)) "==" (second v)))
		     (println (first v) "==" (second v))))
		 (println @(:name-ids *day*))
		 (doseq [t (tuple-browser-seq (.browse (BTree/load (:recman *day*) (first (val (first @(:name-ids *day*)))))))]
		   (println t))
		 (doseq [t (apply interleave-sorted compare-first [(tuple-browser-seq (.browse (BTree/load (:recman *day*) (first (val (first @(:name-ids *day*)))))))])]
		   (println t))
		 (is (= 1 (count @(:name-ids *day*))))
		 (is (= 1 (count (val (first  @(:name-ids *day*))))))
		 (is (empty? @(:browser-recid *day*)))
		 (is (empty? @(:writter-recid-latches *day*)))

		 (with-open [b (browser *day* name)];(apply browser *day* (get (names-and-ids db date) name))]
		   (is (= 1 (count @(:browser-recid *day*))))
		   (is (= (get @(:browser-recid *day*) b) (first (val (first  @(:name-ids *day*))))))
		   (.close b)
		   (is (= 0 (count @(:browser-recid *day*)))))
		 (with-open [b (browser *day* name)];(apply browser *day* (get (names-and-ids db date) name))
		      (let [first-id ( first (val (first  @(:name-ids *day*))))]
			(add-to-db db 42 (Date. (+ 3000 (.getTime date))) name)
			(is (= 1 (count @(:browser-recid *day*))))
			(is (= 2 (count (val (first  @(:name-ids *day*))))))
			(is (= first-id (first (val (first  @(:name-ids *day*))))))
			(is (= (map #(second %) b) [32 37]))
			(is (= (map #(second %) (browser *day* name));(apply browser *day* (get (names-and-ids db date) name))
			        [32 37 42])) )
		   )))))

		  


(defn remove-date [date])

(defn compress-data
  ([date termination-fn])
  ([date] (compress-data date (fn [] false))))

(defn get-from-db [fun day keys-and-data])

