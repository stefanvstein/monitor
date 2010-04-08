(ns se.sj.monitor.daynames
(:use se.sj.monitor.db)
(:use cupboard.bdb.je)
(:use cupboard.utils)
(:use clojure.test)
(:import java.util.Date )
(:import java.text.SimpleDateFormat))

(def dayname-db (atom nil))
(def dayname-cache (atom nil))

(defmacro using-dayname-db
     [db-env name & form]
     `(when-let [db# (db-open ~db-env ~name :allow-create true)]
	(swap! dayname-db (fn [c#] db#))
	(try
	 (do ~@form)
	 (finally (swap! dayname-db (fn [_#] nil ))
		  (db-close db#)))))


(defn add-to-dayname [date names]
  (when-let [db  @dayname-db]
    (if (instance? Date date)
      (add-to-dayname (.format (SimpleDateFormat. date-format) date) names)
      (do
;	(println (str "Adding dayname " (class names) (count names)))
	(swap! dayname-cache (fn [_] [date names]))
	(db-put @dayname-db date names)))))

(defn get-from-dayname [date]
  (when-let [db @dayname-db]
    (if (instance? Date date)
      (get-from-dayname (.format (SimpleDateFormat. date-format) date))
      (do
	(if (= date (first @dayname-cache))
	  (second @dayname-cache)
	  (second (db-get db date)))))))
	  
	   
	  

(deftest dayname-test
  (let [tmp (make-temp-dir)
	df (SimpleDateFormat. "yyyyMMdd HHmmss")
	dparse #(. df parse %)]
    (try
     (using-db tmp "test" [:date :olle :nisse]
	       (using-dayname-db @*db-env* "dayname-test"

				 (is (nil? (get-from-dayname "20071120")))
				 (add-to-dayname "20071120" "Nisse")

				 (is (= "Nisse" (get-from-dayname "20071120")))
				 (add-to-dayname "20071120" "Olle")
				 (is (= "Olle" (get-from-dayname "20071120")))
				 (add-to-dayname "20071120" #{{:a "Hum" :b "Di"} {:a "Ni" :b "Di"}})
				 (is (= #{{:a "Hum" :b "Di"} {:a "Ni" :b "Di"}} (get-from-dayname "20071120")))
				 (is (nil? (get-from-dayname "20071121")))
	       )
)
     (finally  (rmdir-recursive tmp)))))