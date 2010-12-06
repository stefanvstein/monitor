(ns monitor.termination
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

;(def ^{:private true} sleep-latch (CountDownLatch. 1))
(def sleep-latch (CountDownLatch. 1))

(defn terminating? []
  (= 0 (.getCount sleep-latch)))

(defn term-sleep [seconds]
  (.await sleep-latch seconds TimeUnit/SECONDS))

(defn term-sleep-until [date]
    (let [until (.getTime date)
	  millis (- until (System/currentTimeMillis))]
      (loop [left millis]
	(if-not (terminating?)
	  (if (> left 1000)
	    (do
	      (try
		(.await sleep-latch left TimeUnit/MILLISECONDS)
		(catch InterruptedException _))
	      (recur (- until (System/currentTimeMillis)))))))))

(defn stop []
  (.countDown sleep-latch))
