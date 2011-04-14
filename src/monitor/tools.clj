(ns monitor.tools
  (:import (java.util Date Calendar))
  (:import (java.io File IOException FileNotFoundException))
  (:import (java.util.concurrent TimeUnit CountDownLatch))
  (:import (java.text SimpleDateFormat))
  (:use clojure.test)
  (:use [clojure.java.io :only [delete-file]])
  ;(:use [clojure.contrib java-utils])
  )

(defn wait-on-latch [^CountDownLatch latch millis terminate?]
   (let [until (+ 10000 (System/currentTimeMillis))]
     (first (drop-while #(when (instance? InterruptedException %)
			   (when (terminate?)
			     (throw %))
			   (Thread/interrupted)
			   true)
			(repeatedly #(try
				       (.await latch (- until (System/currentTimeMillis)) TimeUnit/MILLISECONDS)
				       (catch InterruptedException e e)))))))

;Slow for testing purposes only
(let [last-d (atom (Date.))]
  (defn fdate [^String string]
    (swap! last-d (fn [_]
		    (let [t (.length string)]
		      (cond (> 7 t)
			    (let [day-part-string (.format (SimpleDateFormat. "yyyyMMdd") @last-d)]
			      (.parse (SimpleDateFormat. "yyyyMMdd HHmmss") (str day-part-string " " string)))
			    (> 9 t)
			    (let [time-par-string (.format (SimpleDateFormat. "HHmmss") @last-d)]
			      (.parse (SimpleDateFormat. "yyyyMMdd HHmmss") (str string " " time-par-string)))
			    :else 
			    (.parse (SimpleDateFormat. "yyyyMMdd HHmmss") string)))))))
  
(defn files-size [file]
  (loop [files [file] size 0]
    (if-let [file (first files)]
      (if (.isDirectory file)
	(if-let [files-listed (seq (.listFiles file))]
	  (recur (apply conj (rest files) files-listed) size)
	  (recur (rest files) size))
	(recur (rest files) (+ size (.length file))))
      size)))

(defn interleave-sorted
  "Lazy interleave of seqs that are expect to be sorted. The result is interleaved accoring to the sorted order, accoring to compare-fn. Each seq is expected to be sorted according to compare-fn"  
  [compare-fn & ss]
  (let [comp-fn (fn [a b] (compare-fn (first a) (first b)))]
    (when-let [seqs (apply list (sort comp-fn (filter identity (map #(when-not (empty? %)
								    %) ss))))]
           (let [f (fn f [seqs]
		(let [first-seq (first seqs)
		      remaining (if-let [remaining-in-first (next first-seq)]
				  (if-let [second-seq (second seqs)]
				    (if (> 0 (comp-fn remaining-in-first second-seq)) 
				      (cons remaining-in-first (next seqs))
				      (sort comp-fn (cons remaining-in-first (next seqs))))
				    (list remaining-in-first))
				  (next seqs))]
		  (if remaining
		    (lazy-seq (cons (first first-seq) (f remaining)))
		    (list (first first-seq)))))]
	(when-not (empty? seqs)
	  (f seqs))))))

;Can protocol and extend-protocol be private?
(defprotocol ^{:private true} DayAsInt
  ^Integer (as-int [this]))

(extend-protocol DayAsInt
  String (as-int [this] (Integer/parseInt this))
  Number (as-int [this] (int this))
  Date (as-int [this] (Integer/parseInt (.format (SimpleDateFormat. "yyyyMMdd") this))))

(defn date-as-day
  ([date]
     (if date
       (as-int date)
       (as-int (Date.)))) 
  ([] (date-as-day nil)))

(def *stime* true)
(defmacro stime [name & expr]
  `(let [start# (when *stime* (. System (nanoTime)))
 	 ret# (do ~@expr)]
     (when *stime* (println (str (print-str "Elapsed time \"")  ~name (print-str "\": ")  (/ (double (- (. System (nanoTime)) start#)) 1000000.0) " msecs")))
     ret#))


(defn causes ([e] 
  (take-while #(not (nil? %)) 
	      (iterate (fn [#^Throwable i] (when i (. i getCause))) 
		       e)))
  ([e exception-class]
     (some #(when (instance? exception-class %)
	      %)
	   (causes e))))

(defn day-by-day
  [date]
  (let [start (doto (. Calendar getInstance ) (.setTime date))
	nextfn (fn nextfn [#^Calendar cal] 
		  (lazy-seq 
		   (cons (. cal getTime) 
			 (nextfn (doto cal (.add java.util.Calendar/DATE 1))))))]
    (nextfn start)))

(defn day-seq [lower upper]
  (let [df (java.text.SimpleDateFormat. "yyyyMMdd")
	day-after-upper (. (doto (. java.util.Calendar getInstance)
			     (.setTime upper)
			     (.add java.util.Calendar/DATE 1)) getTime)
	day-after-upper-as-text (. df format day-after-upper)]
    (take-while #(not 
		  (= day-after-upper-as-text (. df format %))) 
		(day-by-day lower))))


(defn full-days-between
  [#^Date from #^Date to]
  (if (> (.getTime from) (.getTime to))
    (full-days-between to from)
    (if (= (.getTime from)(.getTime to))
      [[from to]]
      (do (map (fn [d]
		 (let [cal (Calendar/getInstance)
		       min-date-of-day (Date. (.getTime (.getTime (doto cal
								    (.setTime d)
								    (.set Calendar/HOUR_OF_DAY (.getMinimum cal Calendar/HOUR_OF_DAY))
								    (.set Calendar/MINUTE (.getMinimum cal Calendar/MINUTE))
								    (.set Calendar/SECOND (.getMinimum cal Calendar/SECOND))
								    (.set  Calendar/MILLISECOND (.getMinimum cal Calendar/MILLISECOND))))))
		       max-date-of-day (Date. (.getTime (.getTime (doto cal
								    (.setTime d)
								    (.set Calendar/HOUR_OF_DAY (.getMaximum cal Calendar/HOUR_OF_DAY))
								    (.set Calendar/MINUTE (.getMaximum cal Calendar/MINUTE))
								    (.set Calendar/SECOND (.getMaximum cal Calendar/SECOND))
								    (.set  Calendar/MILLISECOND (.getMaximum cal Calendar/MILLISECOND))))))
		       the-from (if (> (.getTime from) (.getTime min-date-of-day))
				  from
				  min-date-of-day)
		       the-to (if (< (.getTime to) (.getTime max-date-of-day))
				to
				max-date-of-day)]
		   [the-from the-to]))
		   
			    
	     
	     (day-seq from to))))))

(deftest test-full-days-between
  (let [df (java.text.SimpleDateFormat. "yyyyMMdd HHmmss")
	detailed (java.text.SimpleDateFormat. "yyyyMMdd HHmmss S")]
    (is (= [[(.parse detailed "20071126 130001 000") (.parse detailed "20071126 235959 999")]
	    [(.parse detailed "20071127 000000 000") (.parse detailed "20071127 235959 999")]
	    [(.parse detailed "20071128 000000 000") (.parse detailed "20071128 030101 000")]]
	     (full-days-between (.parse df "20071126 130001") (.parse df "20071128 030101"))))
    (is (= [[(.parse detailed "20071126 130001 000") (.parse detailed "20071126 130001 000")]]
	     (full-days-between (.parse df "20071126 130001") (.parse df "20071126 130001"))))
    (is (= [[(.parse detailed "20071126 130001 000") (.parse detailed "20071126 130002 000")]]
	     (full-days-between (.parse df "20071126 130001") (.parse df "20071126 130002"))))))

(defn apply-for
  ([a]
     (for [at a] (list at)))
  ([a & more]
     (loop [first-time true
	    results a 
	    coming (first more)
	    remaining (next more)]
       (let [fored (for [r results c coming] 
		     (if first-time 
		       (list r c)
		       (concat r (list c))))]
	 (if remaining
	   (recur false fored (first remaining) (next remaining))
	   fored)))))

(defn temp-dir 
  ([location]
     (let [loc (if (not (nil? location))
		 (if (= String (class location))
		   (File. location)
		   location)
		 nil)]
       (let [tf (File/createTempFile "temp-" (str (java.util.UUID/randomUUID)) loc)]
	 (when-not (.delete tf)
	   (throw (IOException.
		   (str "Failed to delete temporary file " (.getAbsolutePath tf)))))
	 (when-not (.mkdir tf)
	   (throw (IOException.
		   (str "Failed to create temporary directory " (.getAbsolutePath tf)))))
	 tf)))
  ([]
     (temp-dir nil)))


      
(defn rm [& files]
  (doseq [file files]
    (trampoline (fn rm [& files]
		(when files
		  (let [fi (first files)]
		  (if (and fi (.exists fi))
		    (if (.isDirectory fi) 
		      (if-let [containing (seq (.listFiles fi))]
			#(apply rm (concat containing files))
			(do (delete-file fi)
			    #(apply rm (next files))))
		      (do (delete-file fi)
			  #(apply rm (next files))))
		    #(apply rm (next files))))))
	      file)))
     
(def *temp-dir*)

(defmacro with-temp-dir [& body]
  `(binding [*temp-dir* (temp-dir)]
     (try (do ~@body)
	  (finally (rm *temp-dir*)))))

(deftest test-recursive-files
  (let [dir (atom nil)] 
    (with-temp-dir
      (reset! dir *temp-dir*)
      (.createNewFile (File. *temp-dir* "Arne"))
      (let [d (File. *temp-dir* "Beril")]
	(.mkdir d)
	(.createNewFile (File. d "Cesar"))))
    (is (not (.exists @dir)))
    ))
    
