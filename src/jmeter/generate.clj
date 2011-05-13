(ns jmeter.generate
  (:import (java.net Socket ))
  (:import (java.text SimpleDateFormat))
  (:import (java.util Date))
  (:import (java.io ObjectInputStream))
  )

(defn generate-some [port]
  
  (let [df (SimpleDateFormat. "yyyyMMdd HHmmss")
        con  (when port
			(with-open [s (java.net.Socket. "localhost" port)
				    ois (java.io.ObjectInputStream. (.getInputStream s))]
			  (.readObject ois)))

        dates (iterate #(Date. (+ (* 15 1000) (.getTime %))) (.parse df "20110109 000000"))]
    (doseq [date (take (* 4 60 24 10) dates) category (range 5) instance (range 10) counter (range 100 115)]
      (when con
        (.add con {"category" (str category) "instance" (str instance) "counter" (str counter)} {date (+ (* (rand counter) instance) (* category  100))})))))
    
       


    