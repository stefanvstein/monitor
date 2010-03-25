(ns se.sj.monitor.commongui
(:import (javax.swing.table AbstractTableModel))
(:import (java.awt Color))
)

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



(defn create-comboaction [combo-models combos add-button]
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
