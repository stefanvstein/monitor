(ns se.sj.monitor.runtime
  (:import (javax.swing UIManager JFrame JButton JOptionPane JMenuBar JMenu JMenuItem 
			JPanel JScrollPane JSplitPane JTable JLabel Box JDialog JComboBox
			JTextField WindowConstants JSpinner SpinnerDateModel SwingUtilities
			DefaultComboBoxModel GroupLayout GroupLayout$Alignment JPopupMenu
			BorderFactory))
  (:import (javax.swing.table TableCellRenderer AbstractTableModel))
  (:import (java.awt Dimension BorderLayout Color FlowLayout Component Point GridLayout 
		     GridBagLayout GridBagConstraints Insets))
  (:import (java.awt.event WindowAdapter ActionListener MouseAdapter))
)

(defn new-runtime-panel []
  (let [panel (JPanel.)
	table-columns (atom [:color])
	table-rows (atom [])
	table-model (proxy [AbstractTableModel] []
		      (getRowCount [] (count @table-rows))
		      (getColumnCount [] (count @table-columns))
		      (getColumnName [column] (name (get @table-columns column))) 
		      (getValueAt [row column]
				  (let [the-keyword (nth @table-columns column)]
				    (if (= 0 column) 
				      (:color (get @table-rows row))
				     (the-keyword (:data (get @table-rows row))))))
		      (getColumnClass [column] (if (= 0 column) Color Object)))
	add-row (fn [data color name] 
		  (let [internal-data {:data data :color color :name name}] 
		    (when-not (some #(= (:name internal-data) (:name %)) @table-rows)
		      (swap! table-rows (fn [rows] (conj rows internal-data)))
		      (.fireTableDataChanged table-model))))

	add-column (fn [k-word]
		     (when-not (some #(= k-word %) @table-columns)
		       (swap! table-columns (fn [cols] (conj cols k-word)))
		       (.fireTableStructureChanged table-model)))

	remove-row (fn [row] 
		     ;TODO Remove from graph
		     (swap! table-rows (fn [rows] 
					 (let [begining (subvec rows 0 row )
					       end (subvec rows (+ 1 row) (count rows))]
					   (println begining)
					   (println end)
					   (if (empty? end)
					     begining
					     (apply conj begining end))
					   )))
		     (.fireTableDataChanged table-model)
		     ;TODO recolor 
		     )
	table (JTable.)
	current-row (atom nil)
	popupMenu (doto (JPopupMenu.)
		    (.add ( doto (JMenuItem. "Delete")
			    (.addActionListener (proxy [ActionListener] []
						  (actionPerformed [event]
								   (println (str "Deleteing " @current-row))
								   (remove-row @current-row)))))))
	popup (fn [event]
		(let [x (.getX event)
		      y (.getY event)
		      row (.rowAtPoint table (Point. x y))]
		  (swap! current-row (fn [_] (.convertRowIndexToModel table row)))
		  (.show popupMenu table x y)))]
))