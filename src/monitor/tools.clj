(ns monitor.tools
  (:import (java.util Date Calendar))
  (:use clojure.test))

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

