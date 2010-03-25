(ns se.sj.monitor.commongui
(:import (javax.swing.table AbstractTableModel))
(:import (java.awt Color))
)

(defn create-table-model [rows columns remove-graph-fn recolor-graph-fn] 
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



