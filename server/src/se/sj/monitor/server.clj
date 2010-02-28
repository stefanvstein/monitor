(ns se.sj.monitor.server
  (:use [clojure.test :only [is deftest]])
  (:use [clojure.stacktrace :only [root-cause]])
  (:require [clojure.contrib.logging :only [info]])
  (:import [java.util.concurrent Executors ExecutorCompletionService TimeUnit]) 
)

(defn info 
  ([message]
     (clojure.contrib.logging/info message))
  ([message exception]
     (clojure.contrib.logging/info message exception)))

(def current-server (atom nil))

(def stop-signal (atom false))

(defn stop []
  (swap! stop-signal ( fn [_] true))
  (when-let [current  @current-server]
    (.interrupt current)))

(def running-tasks (ref {}))

(def tasks-to-start (ref #{}))

(def stopped-tasks (ref #{}))

(defn- causes [#^java.lang.Throwable e] 
  (take-while #(not (nil? %)) 
	      (iterate #(when % (. % getCause)) 
		       e)))
(defonce serve-call-count (atom 0))

(defn serve 
([]
   (swap! serve-call-count #(inc %)) 
   (let [thread-count (atom 0)]
   (when @current-server
     (throw (IllegalStateException. (str (. @current-server getName) " is alreadey serving"))))
   (when-not @stop-signal
     (swap! current-server (fn [_] (Thread/currentThread)))
     (try 
      (when-not (empty? @running-tasks)
	(throw (IllegalStateException. "There are tasks marked as running"))) 
      (dosync (ref-set stopped-tasks (hash-set)))
      (let [restart-agent (agent nil)
	    executor (Executors/newCachedThreadPool 
		      (proxy [java.util.concurrent.ThreadFactory] []
			(newThread [runnable]
				   (swap! thread-count #(inc %))
				   (Thread. runnable 
					    (str "Serve-" @serve-call-count "-" @thread-count)))))
	    executor-service (ExecutorCompletionService. executor)]
	(dorun (map (fn [task]
		      (when-let [future (. executor-service submit #^Callable task)]
			(dosync (alter running-tasks assoc future task)
				(alter tasks-to-start disj task))))
		    @tasks-to-start))
	(while (not @stop-signal) 
	       (try
		(when-let [stopped (. executor-service poll 1 TimeUnit/SECONDS)] ;throws handled
		  (try (. stopped get)
		       (catch Exception e
			 (let [chain (causes e)]
			   (cond
			    (some #(instance? java.util.concurrent.ExecutionException %) chain)
			    (info "Task died with exception." 
				  (. (some 
				      #(if (instance? java.util.concurrent.ExecutionException %)
					 % nil) chain) 
				     getCause))
			    (some #(instance? java.util.concurrent.CancellationException %) chain)
			    nil
			    true (throw e)))))
		  (let [task (get @running-tasks stopped)]	    
		    (dosync 
		     (alter running-tasks dissoc stopped)
		     (alter stopped-tasks conj task))
					;guess this could spawn many threads
		    
		    (send-off restart-agent (fn [_] 
					      (try
					       (Thread/sleep (* 1 1000))
					       (catch InterruptedException _)) 
					      (dosync (alter tasks-to-start conj task))
					      ))))
		
		(dorun (map (fn [c] 
			      (let [future (. executor-service submit #^Callable c)]
				(dosync
				(alter running-tasks assoc future c)
				(alter stopped-tasks disj c)
				(alter tasks-to-start disj c))))
			    @tasks-to-start))
		(catch Exception e
		  (when-not (instance? InterruptedException (root-cause e))
		    (throw e)))))
	
	(info "Shutting down")
	(. executor shutdown)
	(try
	 (. executor awaitTermination 10 TimeUnit/SECONDS) ;throws handled
	 (catch InterruptedException e))
	(when-not (. executor isTerminated)
	  (dorun (map (fn [not-started]
			(when-let [task (get @running-tasks not-started)]
			  (dosync 
			   (alter running-tasks dissoc not-started)
			   (alter stopped-tasks conj task))))
		      (. executor shutdownNow)))
	  (let [done (atom false)
		until (+ (System/currentTimeMillis) 10000)]
	    (while (not @done)
		   (try
		    (. executor awaitTermination 
		       (- until (System/currentTimeMillis)) TimeUnit/MILLISECONDS) ;throws handled
		    (swap! done (fn [_] true))
		    (catch InterruptedException e)))))
	(let [done (atom false)]
	  (while (not @done)
		 (if-let [ future (. executor-service poll)]
		   (do
		     (when-let [task (get @running-tasks future)]
		       (dosync 
			(alter running-tasks dissoc future)
			(alter stopped-tasks conj task)))
		     (try (. future get)
			  (catch Exception e
			    (let [chain (causes e)]
			      (cond
			       (some #(instance? java.util.concurrent.ExecutionException %) chain)
			       (info "Task died with exception." 
				     (. (some 
					 #(if (instance? java.util.concurrent.ExecutionException %)
					    % nil) chain) 
					getCause))
			       (some #(instance? java.util.concurrent.CancellationException %) chain)
			       nil
			       true (throw e))))))
		   (swap! done (fn [_] true)))))
	(when (> 0 (count @running-tasks))
	  (info (str "serve ends with " (count @running-tasks) " running tasks"))))
      (finally (swap! current-server (fn [_] nil))
	       (swap! stop-signal ( fn [_] false)))
      ))))
   
([tasks-to-run]
   (when-not @stop-signal
     (dosync (ref-set tasks-to-start (hash-set)))
     (dorun (map (fn [to-run]
	    (dosync (alter tasks-to-start conj to-run)))
	  tasks-to-run))
   (serve))))

(defn- debug-info
  ([mes]
     (println mes))
  ([mes ex]
     (println mes)
     (println (. ex getMessage))
     (println (. (Thread/currentThread) getName))
     (. ex printStackTrace (java.io.PrintWriter. *out* true)))) 




(deftest test-serve
(comment
  (let [res (atom "")
	fn1 #(swap! res (fn [v] (. v concat "Hej")))]  
    (binding [info debug-info]
      (.start (Thread. #(do (Thread/sleep 4000)
			    (stop))))
      (let [r (with-out-str (serve [fn1]))]
	(is (re-matches #"Shutting down\s" r))
	(is (re-matches #"(Hej){2,}" @res)))))
  (let [res (atom "")
	fn1 #(swap! res (fn [v] (. v concat "Hej")))
	fn2 #(swap! res (fn [v] (. v concat "Hoj")))]  
    (binding [info debug-info]
      (.start (Thread. #(do (Thread/sleep 4000)
			    (stop))))
      (let [r (with-out-str (serve [fn1 fn2]))]
	(is (re-matches #"Shutting down\s" r))
	(is (re-matches #"((Hej)|(Hoj)){4,}" @res)))))

(let [res (atom "")
	fn1 #(swap! res (fn [v] (. v concat "Hej")))
	fn2 #(swap! res (fn [v] (throw (IllegalArgumentException. "Test"))))]  
    (binding [info debug-info]
      (.start (Thread. #(do (Thread/sleep 4000)
			    (stop))))
      (let [r (with-out-str (serve [fn1 fn2]))]
	(is (re-find #"(?s)(.*Task died with exception.*IllegalArgumentException: Test){2,}.*Shutting down\s" r))
	(is (re-matches #"(Hej){2,}" @res)))))
)
(let [res (atom "")
	fn1 #(swap! res (fn [v] (. v concat "Hej")))
	fn2 #(Thread/sleep 12000)]  
    (binding [info debug-info]
      (.start (Thread. #(do (Thread/sleep 1000)
			    (stop))))
      (let [r (with-out-str (serve [fn1 fn2]))]
	(println r)
	(newline)
	(println @res))))
  

  )

