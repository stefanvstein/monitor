(ns jmeter.dbmig
  (:import (java.net Socket ))
  (:import (java.io ObjectInputStream))
  (:use [monitor db])
  )

(defn as-named [names]
  (reduce (fn [result n]
	    (assoc result (name (key n)) (val n))) 
	  {} names))

(defn migrate [port db-path]
  (let [con  (when port
			(with-open [s (java.net.Socket. "localhost" port)
				    ois (java.io.ObjectInputStream. (.getInputStream s))]
			  (.readObject ois)))]
    (using-db-env db-path
                  (println (days-with-data))

                  (doseq [day [20110428]]
                    (println day)
                    (doseq [name (names day)]
                      (println name)
                      (let [r (java.util.TreeMap.)]
                        (try (Thread/sleep 1000) (catch Exception _))
                        (get-from-db (fn [d] (doseq [i d]
                                               (.put r (first i) (second i)))) day name)
                        (if con
                          (.add con (as-named name) r)
                          (println (as-named name) "\n" (take 10 r))
                    )))))))