(ns monitor.server
  (:use [monitor termination])
  (:use [clojure.test :only [is deftest]])
  (:use [clojure.stacktrace :only [root-cause]])
  (:use [clojure.contrib.logging :only [info error]])
  (:import [java.util.concurrent CountDownLatch Executors ExecutorCompletionService TimeUnit ScheduledThreadPoolExecutor])
 )

;(defn info 
;  ([message]
;     (clojure.contrib.logging/info message))
;  ([message exception]
;     (clojure.contrib.logging/info message exception)))
;(defn error
;  ([message]
;     (clojure.contrib.logging/error message))
;  ([message exception]
;     (clojure.contrib.logging/error message exception)))

(def current-server (atom nil))

;(def stop-signal (atom false))



(def running-tasks (ref {}))

(def tasks-to-start (ref #{}))

(def stopped-tasks (ref #{}))

(defn- causes [#^java.lang.Throwable e] 
  (take-while #(not (nil? %)) 
	      (iterate #(when % (. % getCause)) 
		       e)))
(defonce serve-call-count (atom 0))
(def *serve-options* {:seconds-until-interrupt 30
		      :seconds-after-interrupt 10
		      :restart-pause 60})
(defn serve 
([]
   (swap! serve-call-count #(inc %)) 
   (let [thread-count (atom 0)]
   (when @current-server
     (throw (IllegalStateException. (str (. @current-server getName) " is alreadey serving"))))
   (when-not (terminating?)
     (swap! current-server (fn [_] (Thread/currentThread)))  ;Do this and the check above in sam trans
     (try 
      (when-not (empty? @running-tasks)
	(throw (IllegalStateException. "There are tasks marked as running"))) 
      (dosync (ref-set stopped-tasks (hash-set)))
      (let [restart-executor (doto (ScheduledThreadPoolExecutor. 1)
			        (.setExecuteExistingDelayedTasksAfterShutdownPolicy false))
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
	(while (not (terminating?))
	       (try
		(when-let [stopped (. executor-service poll 1 TimeUnit/SECONDS)] ;throws handled
		  (try (. stopped get)
		       (catch Exception e
			 (let [chain (causes e)]
			   (cond
			    (some #(instance? java.util.concurrent.ExecutionException %) chain)
			    (error "Task died with exception." 
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
		    (let [pause (:restart-pause *serve-options*)
			  restart-fn (fn []
				       (dosync (alter tasks-to-start
							     (fn [tasks]
							       (when-not (terminating?)
								 (conj tasks task))))))]
		      (.schedule restart-executor #^Runnable restart-fn (long pause) TimeUnit/SECONDS)
;		      (send-off restart-agent (fn [_] 
;						(try
;						  (term-sleep pause)
;						  (catch InterruptedException _)) 
;						
;						))
		      )))
		
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
	(. restart-executor shutdown)
	(try
	 (. executor awaitTermination 
	    (:seconds-until-interrupt *serve-options*) 
	    TimeUnit/SECONDS) ;throws handled
	 (catch InterruptedException e))
	(when-not (. executor isTerminated)
	  (dorun (map (fn [not-started]
			(when-let [task (get @running-tasks not-started)]
			  (dosync 
			   (alter running-tasks dissoc not-started)
			   (alter stopped-tasks conj task))))
		      (. executor shutdownNow)))
	  (let [done (atom false)
		until (+ (System/currentTimeMillis) 
			 (* 1000 (:seconds-after-interrupt *serve-options*)))]
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
			       (error "Task died with exception." 
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
	       ;(swap! stop-signal (fn [_] false))
	       (info "Is down"))
      
      ))))
   
([tasks-to-run & options]
   (when-not (terminating?)
     (binding [*serve-options* (merge *serve-options* 
				      (apply hash-map options))] 
       (dosync (ref-set tasks-to-start (hash-set)))
       (dorun (map (fn [to-run]
		     (dosync (alter tasks-to-start conj to-run)))
		   tasks-to-run))
       (serve)))))

(defn- debug-info
  ([mes]
     (println mes))
  ([mes ex]
     (println mes)
     (println (. ex getMessage))
     (println (. (Thread/currentThread) getName))
     (. ex printStackTrace (java.io.PrintWriter. *out* true)))) 

(defn- debug-error
  ([mes]
     (println mes))
  ([mes ex]
     (println mes)
     (println (. ex getMessage))
     (println (. (Thread/currentThread) getName))
     (. ex printStackTrace (java.io.PrintWriter. *out* true)))) 


(deftest test-serve
  (println "Hammarby")
  (let [res (atom "")
	fn1 #(swap! res (fn [v] (. v concat "Hej")))]  
    (binding [info debug-info
	      error debug-error
	      monitor.termination/sleep-latch (CountDownLatch. 1)]
      (println "Hu")
      (.start (Thread. #(do (Thread/sleep 4000)
			    (stop))))
      (let [r (with-out-str (serve [fn1]  :restart-pause 1))]
	(is (re-find #"(?s)Shuitting down\s" r))
	
	(is (re-matches #"(Hej){2,}" @res)))))
  (println "Hmm")
  (let [res (atom "")
	fn1 #(swap! res (fn [v] (. v concat "Hej")))
	fn2 #(swap! res (fn [v] (. v concat "Hoj")))]  
    (binding [info debug-info]
      (.start (Thread. #(do (Thread/sleep 4000)
			    (stop))))
      (let [r (with-out-str (serve [fn1 fn2]  :restart-pause 1))]
	(is (re-find #"Shutting down\s" r))
	(is (re-matches #"((Hej)|(Hoj)){4,}" @res)))))

  (let [res (atom "")
	fn1 #(swap! res (fn [v] (. v concat "Hej")))
	fn2 #(swap! res (fn [v] (throw (IllegalArgumentException. "Test"))))]  
    (binding [info debug-info
	      error debug-error]
      (.start (Thread. #(do (Thread/sleep 4000)
			    (stop))))
      (let [r (with-out-str (serve [fn1 fn2] :restart-pause 1))]
	(is (re-find #"(?s)(.*Task died with exception.*IllegalArgumentException: Test){2,}.*Shutting down\s" r))
	(is (re-matches #"(Hej){2,}" @res)))))

(let [res (atom "")
	fn1 #(swap! res (fn [v] (. v concat "Hej")))
	fn2 #(Thread/sleep 3000)]  
    (binding [info debug-info
	      error debug-error]
      (.start (Thread. #(do (Thread/sleep 1000)
			    (stop))))
      (let [r (with-out-str (serve [fn1 fn2]  :restart-pause 1 :seconds-until-interrupt 1 :seconds-after-interrupt 1))]
	(is (re-find #"(?s).*Shutting down.*InterruptedException.*sleep interrupted" r))
	(is (re-matches #"(Hej){1,}" @res))))))

