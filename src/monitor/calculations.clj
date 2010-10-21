(ns monitor.calculations
  (:use [clojure test set] )
  (:use [clojure.contrib import-static])
  (:import (java.util Calendar Date))
  (:import (java.text SimpleDateFormat)))
(import-static java.util.Calendar YEAR MONTH DAY_OF_MONTH HOUR_OF_DAY MINUTE SECOND MILLISECOND)
(def DAY DAY_OF_MONTH)
(def HOUR HOUR_OF_DAY)

(defn- date-seq [date calendar-field n]
  (let [#^Calendar c (doto (Calendar/getInstance)
	    (.setTime date ))
	t (fn t [#^Calendar c]
	    (lazy-seq
	     (cons (.getTime c)
		   (t (doto c
			(.add calendar-field n))))))]
    (t c)))


					; The algoritm supplied has the following args
					; history-start: The first date of the intended history
					; start: The start date of data that is to be calculated
					; end: The end date of range being calculated
					; history: map of date-values available in range start-history and end. 
(defn- sliding [s algorithm history-length history-unit granularity]
  (when (< granularity history-unit )
    (throw (IllegalArgumentException."history unit can not be smaller than granularity")))
  (let [cal (Calendar/getInstance)				  
	smallest-date (fn [d field]
			(.setTime cal d)
			(.set cal MILLISECOND 0)
			(condp = field
			      DAY  (.set cal
				     (.get cal YEAR)
				     (.get cal MONTH)
				     (.get cal DAY)
				     (.getActualMinimum cal HOUR)
				     (.getActualMinimum cal MINUTE)
				     (.getActualMinimum cal SECOND))
			      HOUR (.set cal
				     (.get cal YEAR)
				     (.get cal MONTH)
				     (.get cal DAY)
				     (.get cal HOUR)
				     (.getActualMinimum cal MINUTE)
				     (.getActualMinimum cal SECOND))
			      MINUTE (.set cal
				     (.get cal YEAR)
				     (.get cal MONTH)
				     (.get cal DAY)
				     (.get cal HOUR)
				     (.get cal MINUTE)
				     (.getActualMinimum cal SECOND))
			      SECOND )
			(.getTime cal))
	steps (date-seq (smallest-date (key (first s)) granularity) granularity 1)
	f (fn f [s history steps]
	    (let [#^Date take-from (first steps)
		  #^Date take-until (second steps)
		  
		  taken (reduce #(assoc %1 (key %2) (val %2))
					      (sorted-map)
					      (take-while #(>= (.getTime take-until)
							      (.getTime #^Date (key %)))
							  s))
		  remove-until (.getTime (doto cal
				 (.setTime take-until)
				 (.add history-unit (- 0 history-length))))
				 
		  history (let [removed (apply dissoc history
					       (filter #(< (.getTime #^Date %) (.getTime remove-until)) 
						       (keys (rseq history))))]
				(merge removed taken))
		  values (algorithm remove-until take-from take-until history)
		  remaining (nthnext s (count taken)) ]
	      (if values
		(lazy-seq (if (< 1 (count values))
			      (if remaining
				(let [consume (fn consume [d]
						(lazy-seq (if (next d)
							    (cons (first d) (consume (rest d)))
							    (cons (first d) (f remaining history (next steps) )))))]
				  (cons (first values) (consume (rest values))))
				(let [consume (fn consume [d]
						(lazy-seq (if (next d)
							    (cons (first d) (consume (rest d)))
							    (cons (first d) nil))))]
				  (cons (first values ) (consume (rest values)))))
			      (if remaining
				(cons (first values) (f remaining history (next steps)))
				(cons (first values) nil))))
		(if remaining 
		  (recur remaining history (next steps))
		  (lazy-seq)))))]
	
    (f s (sorted-map) steps)))

    

(defn sliding-average [s history-length history-unit granularity]
  (let [algo (fn [history-start start end history]
	       (if (empty? history)
		 [(first {end 0})]
		 [(first { end
			 (/ (reduce (fn [a b]
				      (+ a (val b)))
				    0 history)
			    (count history))})]))]
    (sliding s algo history-length history-unit granularity)))

(defn sliding-mean [s history-length history-unit granularity]
  (let [algo (fn [history-start start end history]
	       (if (empty? history)
		 [(first {end 0})]
		 [(first {end (nth
			      (sort (vals history))
			      (/ (count history) 2))})]))]
    (sliding s algo history-length history-unit granularity)))

(defn sliding-max [s history-length history-unit granularity]
  (let [algo (fn [history-start start end history]
	       (if (empty? history)
		 [(first {end 0})]
		 [(first {end (apply max (vals history))})]))]
    (sliding s algo history-length history-unit granularity)))

(defn sliding-min [s history-length history-unit granularity]
  (let [algo (fn [history-start start end history]
	       (if (empty? history)
		 [(first {end 0})]
		 [(first {end (apply min (vals history))})]))]
    (sliding s algo history-length history-unit granularity)))



(defn sliding-per- [s granularity tick]
  
  (let [last-value (atom nil)
	tick (condp = tick
		   SECOND 1000
		   MINUTE (* 60 1000)
		   HOUR (* 60 60 1000)
		   DAY (* 24 60 60 1000)
		   (throw (IllegalArgumentException. "Illegal tick")))
	algo (fn [history-start #^Date start #^Date end history]
		
	       (let [within-start (select-keys history
					       (select #(and (<= (.getTime start) (.getTime #^Date %))
							     (> (.getTime end) (.getTime #^Date %)))
						       (set (keys history))))]
		 
		 (if (empty? within-start)
		   nil
		   (let [single-value (= 1 (count within-start))
			 lastv @last-value
			 dist (if single-value
				(if lastv
				  (- (val (first within-start)) (val lastv))
				  1)
				(if lastv
				  (- (val (last history)) (val lastv)) 
				  (+ 1 (- (val (last history)) (val (first within-start ))))))
			 timespan (- (.getTime end) (.getTime start))
			 result (first {end (* tick (/ (double dist) timespan))})]
		     (reset! last-value (last history))
		     [result]))))]
    (sliding s algo 1 granularity granularity)))


(deftest test-only-sliding

  (let [p #(.parse (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss") %)]
    (is (= {}
	   (into {} (sliding
		     {(p "2001-05-01 01:01:01") 1
		      (p "2001-05-01 01:01:04") 3}
		     (fn [ & _] nil)
		     1 SECOND
		     SECOND))))
		     
    (is (= {(p "2011-05-20 02:00:00") 4}
	   (into {} (sliding
		     {(p "2001-05-01 01:01:01") 1
		      (p "2001-05-01 02:01:01") 3}
		     (fn [& _] {(p "2011-05-20 02:00:00") 4})
		     1 SECOND
		     SECOND))))
    (is (= {(p "2011-05-20 02:00:00") 4}
	   (into {} (sliding
		     {(p "2001-05-01 01:01:01") 1
		      (p "2001-05-01 02:01:01") 3}
		     (fn [& _] [(first {(p "2011-05-20 02:00:00") 4})])
		     1 SECOND
		     SECOND))))
    (is (= {(p "2011-05-20 02:00:00") 4
	    (p "2011-05-21 02:00:00") 5}
	   (into {} (sliding
		     {(p "2001-05-01 01:01:01") 1
		       (p "2001-05-01 01:01:03") 1}
		      
		     (fn [& _] [(first {(p "2011-05-20 02:00:00") 4})
				  (first {(p "2011-05-21 02:00:00") 5})])
		     1 SECOND
		     SECOND))))
    (is (= [(first {(p "2011-05-20 02:00:00") 4})
	    (first {(p "2011-05-21 02:00:00") 5})
	    (first {(p "2011-05-20 02:00:00") 4})
	    (first {(p "2011-05-21 02:00:00") 5})]
	     (sliding
	      {(p "2001-05-01 01:01:01") 1
	       (p "2001-05-01 01:01:03") 1}
	      
	      (fn [& _] [(first {(p "2011-05-20 02:00:00") 4})
			   (first {(p "2011-05-21 02:00:00") 5})])
	      1 SECOND
	      SECOND)))
    ))

(deftest test-sliding
  (let [df (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")
	par #(.parse df %)
	form #(.format df %)]
    
    (let [in-data {(par "2011-12-01 00:00:01") 1
		   (par "2011-12-01 00:00:02") 2
		   (par "2011-12-01 00:00:03") 3
		   (par "2011-12-01 00:02:04") 4
		   (par "2011-12-01 00:02:05") 5
		   (par "2011-12-01 00:02:06") 7
		   (par "2011-12-01 00:04:07") 8}]
      (is (= {(par "2011-12-01 00:01:00") 2
	      (par "2011-12-01 00:02:00") 0
	      (par "2011-12-01 00:03:00") 16/3
	      (par "2011-12-01 00:04:00") 0
	      (par "2011-12-01 00:05:00") 8}
	     (into {} (sliding-average in-data 1 MINUTE MINUTE))) "Average 1 min min")
      (is (= {(par "2011-12-01 00:01:00") 2
	      (par "2011-12-01 00:02:00") 0
	      (par "2011-12-01 00:03:00") 5
	      (par "2011-12-01 00:04:00") 0
	      (par "2011-12-01 00:05:00") 8}
	     (into (sorted-map) (sliding-mean in-data 1 MINUTE MINUTE)))
	  "Mean 1 min min")
      (is (= {(par "2011-12-01 00:01:00") 3
	      (par "2011-12-01 00:02:00") 0
	      (par "2011-12-01 00:03:00") 7
	      (par "2011-12-01 00:04:00") 0
	      (par "2011-12-01 00:05:00") 8}
	     (into (sorted-map) (sliding-max in-data 1 MINUTE MINUTE))))
      (is (= {(par "2011-12-01 00:01:00") 1
	      (par "2011-12-01 00:02:00") 0
	      (par "2011-12-01 00:03:00") 4
	      (par "2011-12-01 00:04:00") 0
	      (par "2011-12-01 00:05:00") 8}
	     (into (sorted-map) (sliding-min in-data 1 MINUTE MINUTE))))
      
      (is (= {(par "2011-12-01 00:01:00") 3
	      (par "2011-12-01 00:02:00") 3
	      (par "2011-12-01 00:03:00") 7
	      (par "2011-12-01 00:04:00") 7
	      (par "2011-12-01 00:05:00") 8}
	     (into (sorted-map) (sliding-max in-data 4 MINUTE MINUTE)))))
    (let [in-data {(par "2011-12-01 00:00:02") 2
		   (par "2011-12-01 00:00:04") 6
		   (par "2011-12-01 00:00:06") 8
		   (par "2011-12-01 00:01:06") 10}]
      (is (= {(par "2011-12-01 00:01:00") 3
	      (par "2011-12-01 00:02:00") 2}
	     (into (sorted-map) (sliding-per- in-data MINUTE MINUTE)))))
    (let [in-data {(par "2011-12-01 00:00:01") 8
		   (par "2011-12-01 00:00:02") 2
		   (par "2011-12-01 00:00:03") 3
		   (par "2011-12-01 00:02:04") 4
		   (par "2011-12-01 00:02:05") 5
		   (par "2011-12-01 00:02:06") 7
		   (par "2011-12-01 00:04:07") 1}]
      (is (= {(par "2011-12-01 00:01:00") 8
	      (par "2011-12-01 00:02:00") 8
	      (par "2011-12-01 00:03:00") 8
	      (par "2011-12-01 00:04:00") 8
	      (par "2011-12-01 00:05:00") 8}
	     (into (sorted-map) (sliding-max in-data 1  HOUR MINUTE))))
      )))
