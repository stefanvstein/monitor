(ns monitor.clientdb
  (:import [java.io IOException File]
           [java.util Properties]
           [com.google.common.io Files]
           [jdbm RecordManagerFactory RecordManagerOptions RecordManager])
  (:use [clojure test]))

(defn close-and-remove-db [db-and-directory] 
  (try
    (.close (:db db-and-directory))
    (catch IOException _))
  (try
    (Files/deleteRecursively (:directory db-and-directory))
    (catch IOException _)))

(defn create-db
  ([directory] 
     (let [path (.getPath (doto (File/createTempFile "monitor"
                                                     "Analysis"
                                                     directory)
                            (.delete)))
           db (RecordManagerFactory/createRecordManager path
                                                     (doto (Properties.)
                                                       (.setProperty RecordManagerOptions/THREAD_SAFE (Boolean/toString true))
                                                       (.setProperty RecordManagerOptions/AUTO_COMMIT (Boolean/toString true))
                                                       (.setProperty RecordManagerOptions/CACHE_TYPE "none")))]
     {:db db :directory directory}))
  ([]
     (create-db (Files/createTempDir))))
  

(defn remove-on-terminate
  "Returns a thread that will be executed on termination, that closes a record manager and deleted the directory"
  [db-and-directory]
  (let [t (Thread. (fn [] (close-and-remove-db db-and-directory)))]
    (.addShutdownHook (Runtime/getRuntime) t)
    (assoc db-and-directory :thread t)))

(defn dont-remove-on-terminate [db]
  (when-let [t (:thread db)]
    (.removeShutdownHook (Runtime/getRuntime) t)))

  
    

(deftest first-test
  (let [dir (doto (File. "firsttest") (.mkdir))
        db (remove-on-terminate (create-db dir))
        recman ^RecordManager (:db db)]
    (time (dotimes [i 1000000]
            (.insert recman i)))
    (try
      (dont-remove-on-terminate db)
      (finally (.run (:thread db))))))
        