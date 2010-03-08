(ns jarAll
(:import (java.io File FileOutputStream FileInputStream))
(:import (java.util.jar JarOutputStream Manifest))
(:import (java.util.zip ZipEntry))
(:use (clojure.contrib duck-streams)))


(let [manifest (first *command-line-args*) 
      input (butlast (rest *command-line-args*))
      output (last *command-line-args*)]
(with-open [
      jar (JarOutputStream. (FileOutputStream. output) (Manifest. (FileInputStream. manifest)))]
  (dorun (map (fn [an-input] 
		(let [full-name-len (. (. (File. an-input) getAbsolutePath) length)]
		  (dorun (map (fn [file] 
				(when-not (or (.isDirectory file) 
					      (.isHidden file) 
					      (. (.getName file) endsWith "~"))
				  (. jar putNextEntry (ZipEntry. (. (.getAbsolutePath file) substring full-name-len)))
				  (copy file jar))
				) (file-seq (file-str an-input))))
		  
		  )) input))))



   