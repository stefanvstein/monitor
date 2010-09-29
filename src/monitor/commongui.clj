(ns monitor.commongui
  (:import (javax.swing.table AbstractTableModel))
  (:import (java.util Date))
  (:import (java.awt Color))
  (:import (java.awt.event ActionListener))
  (:import (monitor ServerInterface$Transform ServerInterface$Granularity))
)


(defn color-cycle []
  (let [ colors [(Color. 205, 197, 194) Color/red Color/blue Color/green Color/yellow Color/cyan Color/magenta Color/orange Color/pink (Color. 205 133 063) Color/darkGray (Color. 144 238 144) (Color. 139 0 0) (Color. 139 0 139) (Color. 205 104 057) (Color. 192 255 062) (Color. 238 213 210) (Color. 255 215 000) (Color. 239, 222, 205) (Color. 120, 219, 226) (Color. 135, 169, 107) (Color. 159, 129, 112) (Color. 172, 229, 238) (Color. 162, 162, 208) (Color. 206, 255, 29) (Color. 205, 74, 76) (Color. 142, 69, 133) (Color. 113, 75, 35) ]
	colorcycle (atom (cycle colors))]
    (fn [] (first (swap! colorcycle (fn [cyc] (rest cyc)))))))

(defn names-as-keyworded [names]
  (reduce (fn [result name]
	    (assoc result (keyword (key name)) (val name))) 
	  {} names))

(defn get-names ([from to server]
  (let [raw-names (.rawNames (server) from to)
	names (reduce (fn [result a-map]
			(conj result (names-as-keyworded a-map))) [] raw-names)]
    (reduce (fn [result name]
	      (reduce (fn [r per-subname] 
			(if-let [this-name (get result (key per-subname))]
			  (assoc r (key per-subname) (conj this-name (val per-subname)))
			  (assoc r (key per-subname) [(val per-subname)])))
		      result 
		      (reduce (fn [a subname] (assoc a (key subname) name)) {} name)))
	       (sorted-map) names)))
  ([server]
     (let [raw-names (.rawLiveNames server)
	   names (reduce (fn [result a-map]
			   (conj result (names-as-keyworded a-map))) [] raw-names)]
       (reduce (fn [result name]
		 (reduce (fn [r per-subname] 
			   (if-let [this-name (get result (key per-subname))]
			     (assoc r (key per-subname) (conj this-name (val per-subname)))
			     (assoc r (key per-subname) [(val per-subname)])))
			 result 
			 (reduce (fn [a subname] (assoc a (key subname) name)) {} name)))
	       (sorted-map) names))))

(defn get-data 
  ([from to names func-string granularity-string server]
     (try
       (let [func (condp = func-string
		      "Raw" ServerInterface$Transform/RAW
		      "Average/Minute" ServerInterface$Transform/AVERAGE_MINUTE
		      "Average/Hour" ServerInterface$Transform/AVERAGE_HOUR
		      "Average/Day" ServerInterface$Transform/AVERAGE_DAY
		      "Mean/Minute" ServerInterface$Transform/MEAN_MINUTE
		      "Mean/Hour" ServerInterface$Transform/MEAN_HOUR
		      "Mean/Day" ServerInterface$Transform/MEAN_DAY
		      "Min/Minute" ServerInterface$Transform/MIN_MINUTE
		      "Min/Hour" ServerInterface$Transform/MIN_HOUR
		      "Min/Day" ServerInterface$Transform/MIN_DAY
		      "Max/Minute" ServerInterface$Transform/MAX_MINUTE
		      "Max/Hour" ServerInterface$Transform/MAX_HOUR
		      "Max/Day" ServerInterface$Transform/MAX_DAY
		      "Change/Second" ServerInterface$Transform/PER_SECOND
		      "Change/Minute" ServerInterface$Transform/PER_MINUTE
		      "Change/Hour" ServerInterface$Transform/PER_HOUR)
	     granularity (condp = granularity-string
			     "All Data" ServerInterface$Granularity/SECOND
			     "Minute" ServerInterface$Granularity/MINUTE
			     "Hour" ServerInterface$Granularity/HOUR
			     "Day" ServerInterface$Granularity/DAY)
	     stringed-names (interleave 
			    (map #(name (first %)) (partition 2 names)) 
			    (map #(second %) (partition 2 names)))
	     data (.rawData (server) from to (java.util.ArrayList. stringed-names) func granularity)]
	 (reduce (fn [result a-data] 
		  (assoc result (names-as-keyworded (key a-data)) (val a-data))) 
		{} data))
     (catch Exception e (println e)
	    {})))
  ([names func-string  server]
     (try
       (let [func (condp = func-string
		      "Raw" ServerInterface$Transform/RAW
		      "Average" ServerInterface$Transform/AVERAGE_MINUTE
		      "Mean" ServerInterface$Transform/MEAN_MINUTE
		      "Change/Second" ServerInterface$Transform/PER_SECOND
		      "Change/Minute" ServerInterface$Transform/PER_MINUTE
		      "Change/Hour" ServerInterface$Transform/PER_HOUR)
	     
	     stringed-names-in-hashmaps (reduce 
					(fn [r i]
					  (conj r (java.util.HashMap. 
						   (reduce 
						    (fn [a b]
						      (assoc a (name (key b)) (val b))) 
						    {} i)))
					  ) [] names)
	    data (.rawLiveData (server) (java.util.ArrayList. stringed-names-in-hashmaps) func)]
	(reduce (fn [result a-data] 
		  (assoc result (names-as-keyworded (key a-data)) (merge (sorted-map) (val a-data)))) 
		{} data))
     (catch Exception e (println e) {}))))

(defn create-table-model [remove-graph-fn recolor-graph-fn] 
  "rows and columns is expected to be atom []. Column 0 is expected to be a color"
  (let [rows (atom [])
	columns (atom [:color])
	model (proxy [AbstractTableModel] []
		(getRowCount [] (count @rows))
		(getColumnCount [] (count @columns))
		(getColumnName [column] (name (get @columns column))) 
		(getValueAt [row column]
			    (let [the-keyword (nth @columns column)]
			      (if (= 0 column) 
				(:color (get @rows row))
				(the-keyword (:data (get @rows row))))))
		(getColumnClass [column] (if (= 0 column) Color Object)))
	add-row (fn [data color name] 
		  (let [internal-data {:data data :color color :name name}] 
		    (when-not (some #(= (:name internal-data) (:name %)) @rows)
		      (swap! rows (fn [rows] (conj rows internal-data)))
		      (.fireTableDataChanged model))))
	add-column (fn [k-word]
		     (when-not (some #(= k-word %) @columns)
		       (swap! columns (fn [cols] (conj cols k-word)))
		       (.fireTableStructureChanged model)))
	remove-row (fn [row-num]
		     (remove-graph-fn row-num)
		     (swap! rows (fn [rows] 
				   (let [begining (subvec rows 0 row-num )
					 end (subvec rows (+ 1 row-num) (count rows))]
				     (if (empty? end)
				       begining
				       (apply conj begining end)))))
		     (.fireTableDataChanged model)
		     (dotimes [row-number (count @rows)]
		       (recolor-graph-fn row-number (:color (get @rows row-number)))))] 
	(assoc {} 
	  :model model 
	  :rows rows 
	  :columns columns 
	  :add-row add-row 
	  :add-column add-column 
	  :remove-row remove-row)))



(defn create-comboaction [combo-models combos add-button names]
  (let [place-holder (atom nil)
	action (proxy [ActionListener] []
		 (actionPerformed [event]
				  (let [requirements 
					(reduce (fn [requirements keyword-model]
						  (assoc requirements 
						    (key keyword-model) 
						    (.getSelectedItem (val keyword-model)))) 
						{} 
						(filter #(not (= "" (. (val %) getSelectedItem))) combo-models))]
				    (dorun (map (fn [combo] (.removeActionListener combo @place-holder)) combos))
				    (dorun (map (fn [keyword-model] 
						  (let [current-model (val keyword-model)
							current-keyword (key keyword-model)
							current-name (name current-keyword)
							currently-selected (.getSelectedItem current-model)]
						    (.removeAllElements current-model)
						    (.addElement current-model "")
						    
						    (let [toadd (reduce (fn [toadd row]
									  (if (every? 
									       #(some (fn [a-data] (= % a-data)) row) 
									       (dissoc requirements current-keyword))
									    (let [element (get row current-keyword)]
									      (conj toadd element))
									    toadd))
									#{} (current-keyword names))]
						      (dorun (map (fn [el] (. current-model addElement el)) toadd)))
						    (.setSelectedItem current-model currently-selected))) 
						combo-models))
				    (dorun (map (fn [combo] (.addActionListener combo @place-holder)) combos))
				    (dorun (map (fn [combo] (if (= 1 (.getItemCount combo))
							      (.setEnabled combo false)
							      (.setEnabled combo true))) 
						combos))
				    (if (every? (fn [model]  (= "" (.getSelectedItem model))) (vals combo-models))
				      (.setEnabled add-button false)
				      (.setEnabled add-button true)))))]
				    
    (swap! place-holder (fn [_] action))
    action
))

(defn with-nans [data distance-in-millis]
  (let [without-nans  #(reduce (fn [r i]
				(if (.isNaN (val i))
				  r
				  (conj r i)))
			      [] %)
	last-milli (atom 1)
	with-nans #(reduce (fn [r i]
			     (let [currentMilli (.getTime (key i))]
			       (if (< distance-in-millis (- currentMilli @last-milli))
				 (do
				   (swap! last-milli (fn [_] currentMilli))
				   (assoc r (Date. (- currentMilli 1)) (Double/NaN) (Date. currentMilli) (val i)))
				 (do  (swap! last-milli (fn [_] currentMilli)) (assoc r (key i) (val i))))))
			   (sorted-map) %)]
    (with-nans (without-nans data))))
