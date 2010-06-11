(ns se.sj.monitor.newdata
  (:import [java.nio ByteBuffer])
  (:import [java.io ByteArrayOutputStream DataOutputStream])
  (:import [java.text SimpleDateFormat])
  (:import [java.util Date])
  (:use [clojure test])
  (:use cupboard.bdb.je)
  (:use cupboard.utils)

  (:use se.sj.monitor.db))

(set! *warn-on-reflection* true)

(def index-by-name (ref {}))
(def name-by-index (ref {}))
(def next-index (ref 0))
(def current-day (ref nil))
(def dayindex-db (atom nil))
(def current-days (atom nil))

(defn save-index []
  (when @dayindex-db
  (db-put @dayindex-db @current-day @name-by-index)))

(defn load-day [day]
;(println (str "loading " day ". Current " @current-day))
(when @dayindex-db
  (if-let [the-day (second (db-get @dayindex-db day))]
    (let [by-name (reduce #(assoc %1 (val %2) (key %2)) {} the-day)]
;      (println (str "Loaded " day ", containing " the-day)) 
      (dosync 
       (ref-set name-by-index  the-day)
       (ref-set index-by-name by-name)
       (ref-set current-day day)
       (ref-set next-index (count the-day))
     ))
    (do
 ;     (println (str "Made new " day))
    (dosync 
     (ref-set name-by-index {})
     (ref-set index-by-name {})
     (ref-set current-day day)
     (ref-set next-index 0)
    )))))

(defmacro using-dayindex-db
  [db-env name & form]
  `(when-let [dbii# (db-open ~db-env ~name :allow-create true)]
     (swap! dayindex-db (fn [c#] dbii#))
     (dosync 
      (ref-set index-by-name {})
      (ref-set name-by-index {})
      (ref-set current-day nil)
      (ref-set next-index 0))
     (try
      (do ~@form)
      (finally
       (dosync 
	(ref-set index-by-name {})
	(ref-set name-by-index {})
	(ref-set current-day nil)
	(ref-set next-index 0))
       (swap! dayindex-db (fn [_#] nil ))
       (db-close dbii#)))))

(defn index-for-name [day name]
  (when (not (= @current-day day))
    (load-day day))
  (if-let [index (get @index-by-name name)]
    index
    (do
      (let [result (atom nil)]
	(dosync
	 (alter index-by-name assoc name @next-index)
	 (alter name-by-index assoc @next-index name)
	 (alter next-index inc)
	 (swap! result (fn [l] (dec @next-index))))
	(save-index)
	@result))))
    


(defn name-for-index [day index]
  (when (not (= @current-day day))
    (load-day day))
  (get @name-by-index index))

(def *date-formatter* nil)

(defn new-date-formatter []
  (SimpleDateFormat. "yyyyMMdd"))

(defn to-bytes [data]
  (let [array-stream (ByteArrayOutputStream.)
	data-out (DataOutputStream. array-stream)
	date-format  (if *date-formatter*
		       *date-formatter*
		       (new-date-formatter))
	time-stamp (:time data)
	day (Integer/parseInt (.format #^SimpleDateFormat date-format time-stamp))]
    (when (not (= @current-day day))
      (load-day day))
    (.writeInt data-out day)
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

(defn day-of [bytes]
  (.getInt (ByteBuffer/wrap bytes)))

(defn time-stamp-of [bytes]
  (.getLong (ByteBuffer/wrap bytes) 4))

(defn host-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 20))

(defn category-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 24))

(defn counter-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 28))

(defn instance-index-of [bytes]
  (.getInt (ByteBuffer/wrap bytes) 32))

(defn from-bytes [bytes]
  (let [buffer (ByteBuffer/wrap bytes)
	day (let [a-day (.getInt buffer)]
	      (when (not (= @current-day a-day))
		(load-day a-day))
	      a-day)
	timestamp (Date. (.getLong buffer))
	value (.getDouble buffer)
	host (name-for-index day (.getInt buffer))
	category (name-for-index day (.getInt buffer))
	counter (name-for-index day (.getInt buffer))
	instance (name-for-index day (.getInt buffer))
	additional (int (.get buffer))
	r (assoc {} :time timestamp :value value :host host :counter counter :category category :instance instance)]
    (if (< 0 additional)
      (loop [i 0 v r]
	(if (< i additional)
	  (recur (inc i) (assoc v 
			   (keyword (name-for-index day (.getInt buffer))) 
			   (name-for-index day (.getInt buffer))))
	  v))
      r)))

(deftest testindices-with-persistence
 (let [tmp (make-temp-dir)]
   (try
    (let [df (SimpleDateFormat. "yyyyMMdd HHmmss")]
      (binding [*date-formatter* (new-date-formatter)]
	(let [data (assoc {} :time (.parse df "20071126 033332")  :value 23.0 :host "1" :counter "3" :category "2" :instance "4")
	      another-data (assoc {} :time (.parse df "20071125 033332")  :value 2.0 :host "1" :counter "3" :category "2" :instance "4")
	      some-more-data (assoc {} :time (.parse df "20071125 033332") :value 3.0 :host "1" :counter "33" :category "2" :instance "44")]

	  (using-db tmp "test" [:date :olle :nisse]
		    (using-dayindex-db @*db-env* "dayindex-test"
				       (let [bytes (to-bytes data) ] 
					 (is (= 20071126 (day-of bytes)))
					 (is (= data (from-bytes bytes)))
					 (is (= 23.0 (:value (from-bytes bytes))))
					 (let [another-bytes (to-bytes another-data)]
					   (is (= 20071125 (day-of another-bytes)))
					   (is (= another-data (from-bytes another-bytes)))
					   (is (= 23.0 (:value (from-bytes bytes))))
					   (is (= 2.0 (:value (from-bytes another-bytes))))
					   (is (= 23.0 (:value (from-bytes bytes))))
					   (let [some-more-bytes (to-bytes some-more-data)]
					     (is (= 2.0 (:value (from-bytes another-bytes))))
					     (is (= 3.0 (:value (from-bytes some-more-bytes))))
					     (is (= 23.0 (:value (from-bytes bytes))))
					   (is (= 2.0 (:value (from-bytes another-bytes))))
					     (is (= 3.0 (:value (from-bytes some-more-bytes))))
					     
					 ))))))))
    (finally (rmdir-recursive tmp)))))


(deftest testindices
  (let [df (SimpleDateFormat. "yyyyMMdd HHmmss")]
    (binding [*date-formatter* (new-date-formatter)]
      (let [data (assoc {} :time (.parse df "20071126 033332")  :value 23.0 :host "1" :counter "3" :category "2" :instance "4")]
	
	(let [bytes (to-bytes data)]
	  (is (= 20071126 (day-of bytes)))
	  (is (= (.parse df "20071126 033332") (Date. (time-stamp-of bytes))))
	  (is (= 0 (host-index-of bytes)))
	  (is (= 1 (category-index-of bytes)))
	  (is (= 2 (counter-index-of bytes)))
	  (is (= 3 (instance-index-of bytes)))
	  (is (= data (from-bytes bytes)))))
      (let [data (assoc {} :time (.parse df "20071126 033332")  :value 23.0 :host "1" :counter "3" :category "2" :instance "4" :7 "3")]

	(let [bytes (to-bytes data)]
	  (is (= data (from-bytes bytes))))))))
