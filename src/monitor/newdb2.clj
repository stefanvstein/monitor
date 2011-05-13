(ns monitor.newdb2
  (:import (java.util Date Calendar))
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
  (:use (clojure.contrib profile))
  (:use [clojure.contrib.logging :only [info error]]))
  
(def *db-env* (atom nil)) ;This should be obsolete, moved to the client side

#_(defprotocol DayProtocol
  (names-of-day [this])
  (read-data [this name])
  (close-day [this])
  (closed? [this])
  (write-data [this name date value])
  (sync-db [this])
  (delete-name [this name]))

#_(deftype DayType [name-fn read-fn close-fn closed-fn write-fn sync-fn delete-fn]
  DayProtocol
  (names-of-day [this] (name-fn))
  (read-data [this name] (read-fn name))
  (close-day [this] (close-fn))
  (closed? [this] (closed-fn))
  (write-data [this name date value] (write-fn name date value))
  (sync-db [this] (sync-fn))
  (delete-name [this name] (delete-fn name)))

(defn- tuple-browser-seq
  "Returns a Lazy seq of browsing using a tuple browser"
  [^TupleBrowser tuple-browser]
  (let [f (fn f [^Tuple prev]
            (let [t (Tuple.)]
              (let [status (try (.getNext tuple-browser t))] 
                  (if status
                    (lazy-seq (cons [(.getKey prev) (.getValue prev)] (f t)))
                    (list [(.getKey prev) (.getValue prev)])))))
	t (Tuple.)]
    (when (.getNext tuple-browser t)
      (f t))))

(defn- min-and-max-of-day [date]
  (let [c (Calendar/getInstance)]
	   (doto c
		 (.setTime date)
		 (.set Calendar/HOUR_OF_DAY (.getActualMinimum c Calendar/HOUR_OF_DAY))
		 (.set Calendar/MINUTE (.getActualMinimum c Calendar/MINUTE))
		 (.set Calendar/SECOND (.getActualMinimum c Calendar/SECOND))
		 (.set Calendar/MILLISECOND (.getActualMinimum c Calendar/MILLISECOND)))
           (let [min (.getTime c)]
             (doto c
               (.set Calendar/HOUR_OF_DAY (.getActualMaximum c Calendar/HOUR_OF_DAY))
               (.set Calendar/MINUTE (.getActualMaximum c Calendar/MINUTE))
               (.set Calendar/SECOND (.getActualMaximum c Calendar/SECOND))
               (.set Calendar/MILLISECOND (.getActualMaximum c Calendar/MILLISECOND)))
            [min (.getTime c)])))
  
(defn- day-struct [date create? db-lock db-closed ^String path name-day existing]
  (when @db-closed
    (throw (IllegalStateException. "DB is closed")))
  (let [day (date-as-day date)
        clear-cache-counter (atom 0)]
    (locking db-lock
      (let [create-day (fn create-day []
                         ;(println "create-day " day)
                         (let [min-and-max (min-and-max-of-day (.parse (SimpleDateFormat. "yyyyMMdd") (str day)))
                               min-millis (.getTime ^Date (first min-and-max))
                               max-millis (.getTime ^Date (second min-and-max))
                               write-count (atom 0)
                               r  (try (RecordManagerFactory/createRecordManager
                                        (.getPath (File. path (Integer/toString day))))
                                       (catch IOException e
                                         (throw (IOException. (str "Could not create day " day) e))))
                               id-name-tree  (try (let [k (.getNamedObject r "keys")]
                                               (if (zero? k)
                                                 (let [bt (BTree/createInstance r)]
                                                   (.setNamedObject r "keys" (.getRecid bt))
                                                   bt)
                                                 (BTree/load r k)))
                                                  (catch IOException e
                                                    (throw (IOException. (str "Could not load id-name BTree for " day) e)))) 
                               closed (atom false)
                               read-name-id (fn read-name-id []
                                              (when @closed
                                                (throw (IllegalStateException. (str "Day " day " is closed"))))
                                              (let [browser (.browse id-name-tree)]
                                              (reduce (fn [r e]
                                                        (assoc r (second e) (first e)))
                                                      {}
                                                       (take-while identity (repeatedly (fn [] (let [t (Tuple.)]
                                                                                                 (when (.getNext browser t)
                                                                                                   [(.getKey t) (.getValue t)]))))))))
                               name-id (ref (read-name-id))
;                               lock (Object.)
                               
                               
                               close (fn close []
                                       (locking db-lock
                                         (reset! closed true)
                                         (.commit r)
                                         (try
                                           (.close r)
                                           (catch IOException e
                                             (throw (IOException. (str "Could not close " day ". " (.getMessage e)) e)))
                                           (finally
                                            (dosync (ref-set name-id {})
                                                    (alter name-day dissoc day)
                                                    (reset! closed true))))))

                               read (fn read [name]
                                       (when @closed
                                         #_(throw (IllegalStateException. (str "Day " day " is closed")))
                                         [])
                                       (when-let [id (get @name-id name)]
                                         (locking db-lock
                                           (try
                                             (let [result-seq (if-let [bt (try (BTree/load r id)
                                                                               (catch IOException e
                                                                                 (throw (IOException. (with-out-str (println "Could not open BTree for" name "at" day)) e))))]
                                                                (tuple-browser-seq (.browse ^BTree bt))
                                                                [])]
                                               (reduce (fn [r e]
                                                         (conj r [(Date. (+ min-millis (long (first e))))
                                                                  (second e)]))
                                                       []
                                                       result-seq))
                                             (catch IOException e
                                               (error (str "Could not read " name " from " day "." (.getMessage e)))
                                               [])))))
                               names (fn []
                                       (keys @name-id))
                               create-new-id (fn create-new-id [name]
                                               (let [btree (try (BTree/createInstance r)
                                                                (catch IOException e
                                                                  (throw (IOException. (str "Could not create BTree for " name) e))))
                                                     recid (.getRecid btree)]
                                                 (try
                                                   (.insert id-name-tree recid name true)
                                                   (catch IOException e
                                                     (throw (IOException. (str "Could not save reference to " name " in id-name-tree") e))))
                                                 (dosync (alter name-id assoc name recid))
                                                 recid))
                                               
                               write (fn write [name ^Date date value]
                                       (try
                                       (let [time-in-millis (.getTime date)]
                                         (when (or (> min-millis time-in-millis)
                                                   (< max-millis time-in-millis))
                                           (throw (IllegalArgumentException. (str date " is not " day))))
                                         (locking db-lock
                                           (when @closed
                                             (throw (IllegalStateException. (str "Day " day " is closed"))))
                                       
                                          
                                         (let [id (or (get @name-id name)
                                                      (create-new-id name))
                                               btree (try
                                                       (BTree/load r id)
                                                       (catch IOException e
                                                         (throw (IOException. (str "Could not load BTree for " name " at " day))))) ]
                                           (try (.insert btree (int (- time-in-millis min-millis)) value true)
                                                (catch IOException e
                                                  (throw (IOException. (str "Could not insert into " name " at " day ".") e)))) 
                                           (swap! write-count inc)
                                           (when (< 10000 @write-count)
                                             (.commit r)
                                             (reset! write-count 0))
                                       
                                           )))
                                       (catch IOException e
                                         (error (str "Could not write to " name " at " date ". " (.getMessage e)) e))))
                            ;   clear-cache-counter (atom 10)
                               sync (fn sync []
                                      (when @closed
                                         (throw (IllegalStateException. (str "Day " day " is closed"))))
                                      (locking db-lock
                                        (when-not @closed
                                          (try
                                            (.commit r)
                                            (when (= 10 (swap! clear-cache-counter (fn [c]
                                                                                     (if (> 0 c)
                                                                                       10
                                                                                       (dec c)))))
                                              (.clearCache r))
                                            (reset! write-count 0)
                                            (catch IOException e
                                              (throw (IOException. (str "Could not sync " day ". " (.getMessage e)), e)))))))
                               
                               delete-name (fn delete-name []
                                             (when @closed
                                               (throw (IllegalStateException. (str "Day " day " is closed"))))
                                             (locking db-lock))]
                           (dosync (alter existing conj day))
                              
                           {:names names
                            :read read
                            :write write
                            :close close
                            :closed (fn [] @closed) 
                            :delete delete-name
                            :sync sync}
                           ))]
               
        (if-let [rval (get @name-day day)]
          rval
          (when-not (and (not (contains? @existing day))
                         (not create?))
            (let [rval (create-day)]
              (dosync (alter name-day assoc day rval))
              rval)))))))

(defn db-struct [^String path]
  (let [lock (Object.)
        name-day (ref {})
        existing (ref (reduce (fn [r ^File e]
                                (if-let [m  (re-matches #"^(\d{8})\..*" (.getName e))]
                                  (conj r (Integer/valueOf ^String (second m)))
                                  r))
                              #{}
                              (.listFiles (File. path) (proxy [FileFilter] []
                                                         (accept [^File file]
                                                           (not (.isDirectory file)))))))
        closed (atom false)
        close (fn close-db []
                (locking lock
                  (reset! closed true)
                  (doseq [n-d @name-day]
                    (try
                      ((:close (val n-d)))
                      (catch IOException e
                        (error (str "Could not close " (key n-d)) e))))
                  (when-let [not-closed (keys @name-day)]
                    (info (str not-closed " was not closed")))))
                    
        being-deleted (ref #{})
        delete (fn delete-day [date]
                 (let [day (date-as-day date)]
                   (locking lock
                     (when-let [to-close (dosync (alter being-deleted conj day)
                                                 (let [to-close (get @name-day day)]
                                                   (alter name-day dissoc day)
                                                   to-close))]
;                       (println "To-close is " to-close)

                         ((:close to-close)))
                     (let [files-to-delete (.listFiles (File. path)
                                                       (proxy [FilenameFilter] []
                                                         (accept [dir file-name]
                                                           (.startsWith ^String file-name (Integer/toString day)))))]
                       (doseq [^File file files-to-delete]
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
                       (dosync (alter existing disj day)) 
                       (doseq [^File file files-to-delete]
                         (when-not (.delete file)
                           (throw (IOException. "Could not delete" (.getPath file))))))
                     (dosync (alter being-deleted disj day)))))
        ]
    
    {:close close
     :lock lock
     :path path
     :existing existing
     :delete delete
     :day (fn [date create?]
            (day-struct date create? lock closed path name-day existing))}))



(def *db* nil)
(defmacro using-db [path & body]
  `(let [db# (db-struct ~path)]
     (try
       (binding [*db* db#]
         (do ~@body))
       (finally ((:close db#))))))
       

(deftest simple-test
  (let [d (.parse (SimpleDateFormat. "yyyyMMdd HHmmss") "20010102 010101")
        d2 (.parse (SimpleDateFormat. "yyyyMMdd HHmmss") "20010102 010102")]
    (with-temp-dir
      (let [db-s (db-struct (.getPath *temp-dir*))]
        (try
          (let [day ((:day db-s) d true)]
            ((:write day) {:a "Adam"} d 3.1)
            ((:write day) {:a "Bertil"} d 3.3)
            ((:write day) {:a "Adam"} d2 3.2)
            ((:sync day))
            (is (= [[d 3.1][d2 3.2]] ((:read day)  {:a "Adam"})))
            (is (= [{:a "Bertil"} {:a "Adam"}] ((:names day))))
            ((:close db-s))
            (is ((:closed day)))
            
            (is (thrown? IllegalStateException  ((:write day) {:a "Adam"} d 3.1)))
            (is (thrown? IllegalStateException  ((:sync day))))
            ;(is (thrown? IllegalStateException  ((:read day) {:a "Adam"})))
   
            (is (thrown?  IllegalStateException ((:day db-s) d false)))


            )
          
          (finally ((:close db-s))))))
    (with-temp-dir
      (using-db (.getPath *temp-dir*)
                (let [day ((:day *db*) d true)]
            ((:write day) {:a "Adam"} d 3.1)
            ((:write day) {:a "Bertil"} d 3.3)
            ((:write day) {:a "Adam"} d2 3.2)
            ((:sync day))
            (is (= [[d 3.1][d2 3.2]] ((:read day)  {:a "Adam"})))
            ((:close day))
            (is ((:closed day)))
            (is (thrown? IllegalStateException  ((:write day) {:a "Adam"} d 3.1)))
            (is (thrown? IllegalStateException  ((:sync day))))
            ;(is (thrown? IllegalStateException  ((:read day) {:a "Adam"})))
            (println "day again")
            (let [day-again ((:day *db*) d false)]
              (is (= [[d 3.1][d2 3.2]] ((:read day-again)  {:a "Adam"})))
              )
            (let [non-existing-dat (.parse (SimpleDateFormat. "yyyyMMdd") "20010103")]
              (is (nil? ((:day *db*) non-existing-dat false)))
              ((:delete *db*) non-existing-dat) 
              )
            ((:delete *db*) d)
            ;(is (thrown? IllegalStateException  ((:read day) {:a "Adam"})))
            (println (:existing *db*))
            )))))
                    
  


(defn day-reuse [db-struct to-keep]
 
 
               (let [lock (:lock db-struct)
                     day-users (ref {})
                     day-fifo (ref [])
                     old-ones (fn [] (filter #(not (contains? @day-users %)) (drop-last to-keep @day-fifo)))
                     close-and-remove (fn [to-close] (doseq [d to-close]
                                                 ((:close ((:day db-struct) d false))))
                                  (dosync (alter day-fifo (fn [day-fifo]
                                                            (into [] (filter #(not (some (set to-close) [%])) day-fifo))))))
                     closed? (fn [date]
                               (let [day (date-as-day date)]
                                 (locking lock
                                   (if-not (contains? @day-users day)
                                     (do
                                       (when (some (set @day-fifo) [day])
                                         (close-and-remove [day]))
                                       true)
                                     false))))
                                     
                                       
                     acquire-day (fn acquire-day [date create?]
                                   (locking lock
                                     (let [day (date-as-day date)]
                                       (when-let [d ((:day db-struct) date create?)] 
                                         (close-and-remove (dosync (alter day-fifo (fn [day-fifo]
                                                                                  (conj (into [] 
                                                                                              (filter #(not (= day %)) day-fifo))
                                                                                        day)))
                                                                (alter day-users update-in [day] #(if % (inc %) 1))
                                                                (old-ones)))
                                         d))))
                     release-day (fn release-day [date]
                                   (let [day (date-as-day date)]
                                     (locking lock
                                       (close-and-remove (dosync (alter day-users (fn [day-users]
                                                                                 (if-let [v (get day-users day)]
                                                                                   (if (= 1 v)
                                                                                     (dissoc day-users day)
                                                                                     (assoc day-users day (dec v)))
                                                                                   day-users)))
                                                              (old-ones))))))]
                 
                 {:closed? closed?
                  :acquire acquire-day
                  :release release-day
                  :users (fn [] @day-users)
                  :fifo (fn [] @day-fifo)
                  :db db-struct}))
                 
(deftest test-reuse
  
  (with-temp-dir
    (let [da (fn [s] (.parse (SimpleDateFormat. "yyyyMMdd HHmmss") s))
          db (db-struct (.getPath *temp-dir*))
          reuse (day-reuse db 3)]
      ((:acquire reuse) (da "20070101 234530") true)
      ((:acquire reuse) (da "20070102 234530") true)
      ((:acquire reuse) (da "20070103 234530") true)
      (is (= {20070101 1 20070102 1 20070103 1} ((:users reuse))))
      (is (= [20070101 20070102 20070103] ((:fifo reuse))))
      ((:acquire reuse) (da "20070104 234530") true)
      (is (= {20070101 1 20070102 1 20070103 1 20070104 1} ((:users reuse))))
      (is (= [20070101 20070102 20070103 20070104]  ((:fifo reuse))))


      ((:release reuse) (da "20070101 234530"))
      (is (= {20070102 1 20070103 1 20070104 1} ((:users reuse))))
      (is (= [20070102 20070103 20070104]  ((:fifo reuse))))


      ((:release reuse) (da "20070103 234530"))
      (is (= {20070102 1 20070104 1} ((:users reuse))))
      (is (= [20070102 20070103 20070104]  ((:fifo reuse))))

      ((:acquire reuse) (da "20070105 234530") true)
      (is (= {20070102 1 20070104 1 20070105 1} ((:users reuse))))
      (is (= [20070102 20070103 20070104 20070105]  ((:fifo reuse))))


      ((:acquire reuse) (da "20070106 234530") true)
      (is (= {20070102 1 20070104 1 20070105 1 20070106 1} ((:users reuse))))
      (is (= [20070102 20070104 20070105 20070106]  ((:fifo reuse))))

      ((:release reuse) (da "20070102 000000"))
      (is (= {20070104 1 20070105 1 20070106 1} ((:users reuse))))
      (is (= [20070104 20070105 20070106]  ((:fifo reuse))))

      ((:acquire reuse) (da "20070104 120000") true)
      (is (= {20070104 2 20070105 1 20070106 1} ((:users reuse))))
      (is (= [20070105 20070106 20070104]  ((:fifo reuse))))
      ((:acquire reuse) (da "20070107 120000") true)
      ((:acquire reuse) (da "20070108 120000") true)
      ((:release reuse) (da "20070105 000000"))
      ((:release reuse) (da "20070106 000000"))
      ((:release reuse) (da "20070104 000000"))
      (is (= {20070104 1 20070107 1 20070108 1} ((:users reuse))))
      (is (= [20070104 20070107 20070108]  ((:fifo reuse))))

      )))

(def *day* nil)
(defmacro acquire-and-release [reuse date create? & form]
  `(do
     (binding [*day* ((:acquire ~reuse) ~date ~create?)]
       (try
         (do ~@form)
         (finally ((:release ~reuse) ~date))))))

                                        ; USwer interface

(defmacro using-db-env [path & body]
  `(using-db ~path
             (reset! *db-env* (day-reuse *db* 10))
             (try
               (do ~@body)
               (finally (reset! *db-env* nil)))))



(defn days-with-data []
;  (println "days-with-data")
     (when-let [db (:db @*db-env*)]
       (let [r @(:existing db)]
 ;        (println "Existing days " r)
         r)))


(defn names
  [day]
     (when-let [env @*db-env*]
       (acquire-and-release env day false
                            (if *day*
                              ((:names *day*))
                              ))))

(defn add-to-db
  [value time keys]
  ;(println "add-to-db")
  (when-let [env @*db-env*]
    (acquire-and-release env time true
                         ((:write *day*) keys time value))))

(defn sync-database
  []
  (when-let [env @*db-env*]
    (doseq [day ((:fifo env))]
      (acquire-and-release env day false
                           ((:sync *day*))))))

(defn remove-date
  [date]
  (when-let [env @*db-env*]
    (when ((:closed? env) date)
      ((:delete (:db env)) date))))


(defn get-from-db
  [fun day keys]
  (when-let [env @*db-env*]
    (acquire-and-release env day false
                         (when *day*
                           (let [r ((:read *day*) keys)]
                             (doseq [e (seq r)]
                               (fun [e])))))))


(defn compress-data
  ([date termination-fn])
  ([date] (compress-data date (fn [] false))))
     


(deftest by-interface
  (println "by interface")
   (with-temp-dir
     (let [da (fn [s] (.parse (SimpleDateFormat. "yyyyMMdd HHmmss") s))]
       (using-db-env (.getPath *temp-dir*)
                     (remove-date (da "20010202 000000"))
                     (println (names (da "20010202 000000")))
                     
                     (add-to-db 32 (da "20010202 000001") {:arne "Weise"})
                     (add-to-db 37 (da "20010202 000002") {:arne "Weise"})
                     (add-to-db 38 (da "20010202 000003") {:arne "Weise"})
                     (add-to-db 48 (da "20010201 000003") {:arne "Bodil"})
                     (get-from-db (fn [p] (println p)) (da "20010202 000003") {:arne "Weise"})
                     (println "names" (names (da "20010202 000002")))
                      (sync-database)
                     (println (days-with-data)) 
                    
       ))))
