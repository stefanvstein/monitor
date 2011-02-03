(ns monitor.commongui
  (:use (monitor calculations))
  (:use [clojure.contrib import-static])
  (:import (javax.swing.table AbstractTableModel TableRowSorter))
  (:import (java.util Date Comparator))
  (:import (java.text DecimalFormat))
  (:import (java.awt Color))
  (:import (java.awt.event ActionListener))
  )

(import-static java.util.Calendar MINUTE SECOND)

(def new-window-fn (atom (fn [_])))

(defn color-cycle []
  (let [ colors [(Color. 205, 197, 194) Color/red Color/blue Color/green Color/yellow Color/cyan Color/magenta Color/orange Color/pink (Color. 205 133 063) Color/darkGray (Color. 144 238 144) (Color. 139 0 0) (Color. 139 0 139) (Color. 205 104 057) (Color. 192 255 062) (Color. 238 213 210) (Color. 255 215 000) (Color. 239, 222, 205) (Color. 120, 219, 226) (Color. 135, 169, 107) (Color. 159, 129, 112) (Color. 172, 229, 238) (Color. 162, 162, 208) (Color. 206, 255, 29) (Color. 205, 74, 76) (Color. 142, 69, 133) (Color. 113, 75, 35) ]
	colorcycle (atom (cycle colors))]
    (fn [] (first (swap! colorcycle (fn [cyc] (rest cyc)))))))

(defn names-as-keyworded [names]
  (reduce (fn [result name]
	    (assoc result (keyword (key name)) (val name))) 
	  {} names))

(defn get-names
  ([from to server]
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

(defn transform
  [data func]
     (let [fun (condp = func
		   "Raw" (fn [e] e)
		   "Average/Minute" (fn [e] (sliding-average e 1 MINUTE MINUTE))
		   "Average" (fn [e] (sliding-average e 1 MINUTE MINUTE))
		   "Average/Hour" (fn [e] (sliding-average e 1 HOUR HOUR))
		   "Average/Day" (fn [e] (sliding-average e 1 DAY DAY))
		   "Mean/Minute" (fn [e] (sliding-mean e 1 MINUTE MINUTE))
		   "Mean" (fn [e] (sliding-mean e 1 MINUTE MINUTE))
		   "Mean/Hour" (fn [e] (sliding-mean e 1 HOUR HOUR))
		   "Mean/Day" (fn [e] (sliding-mean e 1 DAY DAY))
		   "Min/Minute" (fn [e] (sliding-min e 1 MINUTE MINUTE))
		   "Min/Hour" (fn [e] (sliding-min e 1 HOUR HOUR))
		   "Min/Day" (fn [e] (sliding-min e 1 DAY DAY))
		   "Max/Minute" (fn [e] (sliding-max e 1 MINUTE MINUTE))
		   "Max/Hour" (fn [e] (sliding-max e 1 HOUR HOUR))
		   "Max/Day" (fn [e] (sliding-max e 1 DAY DAY))
		   "Change/Second" (fn [e] (sliding-per- e SECOND SECOND))
		   "Change/Minute" (fn [e] (sliding-per- e MINUTE MINUTE))
		   "Change/Hour" (fn [e] (sliding-per- e HOUR HOUR))
		   (throw (IllegalArgumentException. (str func " not yet implemented"))))]
       (if (not= func "Raw")
	 (do
	   ;(println (first data))
	 (into (sorted-map) (fun data)))
	 data)))

(defn get-data 
  ([from to names server]
       (let [stringed-names (interleave 
			    (map #(name (first %)) (partition 2 names)) 
			    (map #(second %) (partition 2 names)))
	     data (.rawData (server) from to (java.util.ArrayList. stringed-names))]
	 (if (seq data)
	   (reduce (fn [result a-data]
					;(println a-data)
		     (assoc result
		       (names-as-keyworded (key a-data))
					;(transform (val a-data) func-string)))
		       (val a-data))) 
		   {} data)
	   {})))
  ([names func-string  server]
     (try
       (let [stringed-names-in-hashmaps (reduce 
					(fn [r i]
					  (conj r (java.util.HashMap. 
						   ^java.util.Map (reduce 
						    (fn [a b]
						      (assoc a (name (key b)) (val b))) 
						    {} i)))
					  ) [] names)
	    data (.rawLiveData (server) (java.util.ArrayList. stringed-names-in-hashmaps))]
	(reduce (fn [result a-data] 
		  (assoc result (names-as-keyworded (key a-data)) (merge (sorted-map) (transform (val a-data) func-string)))) 
		{} data))
     (catch Exception e  (throw e)))))

(defn create-table-model [remove-graph-fn recolor-graph-fn] 
  "rows and columns is expected to be atom []. Column 0 is expected to be a color"
  (let [rows (atom [])
	columns (atom [:color :shown :value])
	is-visible (fn is-visible [rows row]
					;((:visible (get rows row)) (:data (get rows row))))
		     ((:visible (get rows row))))
		     
	model (proxy [AbstractTableModel] []
		(getRowCount [] (count @rows))
		(getColumnCount [] (count @columns))
		(getColumnName [column] (name (get @columns column))) 
		(getValueAt [row column]
			      (condp = column
				  0 (:color (get @rows row))
				  1 (is-visible @rows row)
				  2 (:value (get @rows row))
				  ((nth @columns column) (:data (get @rows row)))))
		(getColumnClass [column] (condp = column
					     0 Color
					     1 Boolean
					     2 Number
					     Object))
		(isCellEditable [row column] (= column 1))
		(setValueAt [value row column]
			    (if (= column 1)
			      ((:visible (get @rows row)) value)
			      (throw (IllegalStateException.)))))
	num-of-name (fn num-of-name [name]
		      (let [internal-row (some #(when (= (:data %) name)
							       %)
							       @rows)]
			(let [n (.indexOf @rows internal-row)]
			  (when (<= 0 n)
			    n))))
	add-row (fn [data color visible-fn] ;data-as-comparable is needed by freechart, and should hence not be here. Use a map in analysis instead, where data is key.
;		  (println "Adding-row" name)
		  (let [fake-visible-flag (atom true)
			fake-visible-fn (fn ([]  @fake-visible-flag)
					  ([value]  (reset! fake-visible-flag value)))
			internal-data {:data data :color color  :visible (if visible-fn
										     visible-fn
										     fake-visible-fn)}] 
		    (when-not (some #(= (:data internal-data) (:data %)) @rows)
		      (swap! rows (fn [rows] (conj rows internal-data)))
		      (.fireTableDataChanged model))))
	add-column (fn [k-word]
		     (when-not (some #(= k-word %) @columns)
		       (swap! columns (fn [cols] (conj cols k-word)))
		       (.fireTableStructureChanged model)))

	remove-row (fn [row]
		     (let [row (if (integer? row)
				 (:data (get @rows row))
				 row)
			   row-num (num-of-name row)]

		       (when (and row row-num)
			 (remove-graph-fn row)
			 (swap! rows (fn [rows] 
				       (let [begining (subvec rows 0 row-num )
					     end (subvec rows (+ 1 row-num) (count rows))]
					 (if (empty? end)
					   begining
					   (apply conj begining end)))))
			 (.fireTableDataChanged model)
			 (dotimes [row-number (count @rows)]
			   (recolor-graph-fn row-number (:color (get @rows row-number)))))))
	
	set-value (fn [data value]
		    (let [v value]
		    (swap! rows (fn [rows]
				  (into [] (map (fn [d]
						  (if (= (:data d) data)
						    (assoc d :value v)
						    d)) rows)))))
		    (.fireTableDataChanged model))] 
	(assoc {} 
	  :model model 
	  :rows rows 
	  :columns columns 
	  :add-row add-row 
	  :add-column add-column 
	  :remove-row remove-row
	  :set-value set-value)))



(defn create-comboaction [combo-models combos add-buttons names]
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
				      (dorun (map #(.setEnabled % false) add-buttons))
				      (dorun (map #(.setEnabled % true) add-buttons)) 
				      ))))]
				    
    (reset! place-holder action)
    action
))

(defn isNan [n]
  (or (and (instance? Double n) (.isNaN ^Double n))
      (and (instance? Float n) (.isNaN ^Float n))))

(defn without-nans [m]
  (reduce (fn [r i]
	    (let [value (val i)]
	      (if (isNan value)
		r
		(conj r i))))
	  (sorted-map) m))

(defn with-nans [data distance-in-millis]
  (let [with-nans (fn [d]
		    (when-let [ds (seq d)]
		    (loop [result (sorted-map)
			   last-milli 1
			   i ds]
		      (let [current (first i)
			    currentValue (val current)
			    currentDate (key current)
			    currentMilli (.getTime ^java.util.Date currentDate)
			    remaining (next i)
			    resulting (if (< distance-in-millis (- currentMilli last-milli))
					(assoc result (Date. ^Long (- currentMilli 1)) (Double/NaN) (Date. currentMilli) currentValue)
					(assoc result currentDate currentValue))]
					  
			(if remaining 
			    (recur resulting
				   currentMilli
				   remaining)
			   resulting)))))]
			  
				 
		      
    (with-nans (without-nans data))))

(defn create-row-sorter-with-Number-at [model n]
  (proxy [TableRowSorter] [model]
    (modelStructureChanged []
			   (proxy-super  modelStructureChanged)
			   (proxy-super
			    setComparator n (proxy [Comparator] []
					      (compare [^Number a ^Number b]
						       (cond
							(and (instance? Number a)
							     (instance? Number b))  (let [r (- a b)]
										      (cond
										       (< 0 r) 1
										       (> 0 r) -1
										       :else 0))
							     
							     (instance? Number a) 1
							     (instance? Number b) -1
							     :else 0)))))))