(ns user)
(let [a {:n (- (+ Integer/MAX_VALUE 1) 1)} 
      b {:n Integer/MAX_VALUE}]
  (when (= a b)
    (when (= (hash a) (hash b))
      (= {a :z} {b :z}))))

(defn dissoc-first-found 
  [m s]
  (loop [s s]
    (if ((first s) m) 
      (dissoc m (first s))
      (when (next s)
	(recur (rest s))))))
