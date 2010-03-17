(import (java.net Socket))
(import (java.io ObjectInputStream))

(defn client []
  (with-open [s (Socket. "localhost" 3030)
	      in (ObjectInputStream. (.getInputStream s))]
    (let [r (.readObject in)]
      (println (.ping r )))))

(client)