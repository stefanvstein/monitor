(ns se.sj.monitor.analysis
  (:use (clojure stacktrace))
  (:use (se.sj.monitor commongui))
  (:import (javax.swing UIManager JFrame JButton JOptionPane JMenuBar JMenu JMenuItem 
			JPanel JScrollPane JSplitPane JTable JLabel Box JDialog JComboBox
			JTextField WindowConstants JSpinner SpinnerDateModel SwingUtilities
			DefaultComboBoxModel GroupLayout GroupLayout$Alignment JPopupMenu
			BorderFactory))

  (:import (javax.swing.table TableCellRenderer AbstractTableModel))
  (:import (java.awt Dimension BorderLayout Color FlowLayout Component Point GridLayout 
		     GridBagLayout GridBagConstraints Insets))
  (:import (java.awt.event WindowAdapter ActionListener MouseAdapter))
  (:import (java.net Socket UnknownHostException))
  (:import (java.io ObjectInputStream))
  (:import (org.jfree.chart ChartFactory ChartPanel JFreeChart))
  (:import (org.jfree.data.time TimeSeries TimeSeriesCollection TimeSeriesDataItem Millisecond)))


(defn on-update [names combomodels-on-center centerPanel add-button]
  (let [combos-on-center (atom [])]
    (dorun (map #(do (. centerPanel remove %)) (.getComponents centerPanel)))
    (swap! combomodels-on-center (fn [_] {}))
    (swap! combos-on-center (fn [_] []))
    (.setLayout centerPanel (GridBagLayout.))
    (let [labelsOnCenter (atom [])]
      (dorun (map (fn [a-name]
		    (let [field-name (name (key a-name))
			  label (JLabel. field-name JLabel/RIGHT)
			  comboModel (DefaultComboBoxModel.)
			  combo (JComboBox. comboModel)]
		      (. comboModel addElement "")
		      (let [toadd (reduce (fn [toadd i]
					    (if-let [a-value ((key a-name) i)]
					      (conj toadd a-value)
					      toadd))
					  #{} (val a-name))]
			(dorun (map (fn [el] (. comboModel addElement el)) toadd)))  
		      (. combo setName field-name)
		      
		      (. centerPanel add label (GridBagConstraints. 
						0 GridBagConstraints/RELATIVE 
						1 1 
						0 0 
						GridBagConstraints/EAST 
						GridBagConstraints/NONE 
						(Insets. 1 1 0 4) 
						0 0 ))
		      (. centerPanel add combo (GridBagConstraints. 
						1 GridBagConstraints/RELATIVE 
						1 1 
						0 0 
						GridBagConstraints/WEST 
						GridBagConstraints/NONE 
						(Insets. 0 0 0 0) 
						0 0 ))
		      (swap! labelsOnCenter (fn [l] (conj l label)))
		      (swap! combos-on-center (fn [l] (conj l combo)))
		      (swap! combomodels-on-center (fn [l] (assoc l (key a-name) comboModel)))))
		  names)))
    (let [combo-action (create-comboaction @combomodels-on-center @combos-on-center add-button names)]
      (dorun (map (fn [combo] (.addActionListener combo combo-action)) @combos-on-center))
    )))

			
(defn on-add-to-analysis [from to name-values graph colors chart add-to-table add-column server]
  (.start (Thread. #(let [result (get-data from
					   to
					   name-values server)]
		      (SwingUtilities/invokeLater 
		       (fn [] 
			 (.setNotify chart false)
			 (dorun (map (fn [timeseries] (.setNotify timeseries false)) (. graph getSeries)))
			 (dorun (map (fn [data]
				       (let [time-serie 
					     (if-let 
						 [serie 
						  (. graph getSeries (str (key data)))]
					       serie
					       (let [serie (TimeSeries. (str (key data)))
						     color (colors)]
						 (.setNotify serie false)
						 (. graph addSeries serie)
						 
						 (.. chart 
						     (getPlot) 
						     (getRenderer) 
						     (setSeriesPaint 
						      (- 
						       (count (.getSeries graph)) 
						       1) 
						      color))
						 
						 (add-to-table (key data) color (str (key data)))
						 (dorun (map (fn [i] (add-column i)) (keys (key data)))) 
						 serie))]
					 (let [tempSerie  (TimeSeries. "")]
					   (.setNotify tempSerie false)
					   (let [new-timeserie 
						 (reduce (fn [toAdd entry]
							   (.add toAdd (TimeSeriesDataItem. 
									(Millisecond. (key entry)) 
									(val entry)))
							   toAdd)
							 tempSerie (val data))]
					     
					     (.addAndOrUpdate time-serie new-timeserie)))))
				     result))
			 (dorun (map (fn [timeseries] (.setNotify timeseries true)) (. graph getSeries)))
			 (.setNotify chart true))))
		   "Data Retriever")))


(defn analysis-add-dialog [contents server]
  (let [dialog (JDialog. (SwingUtilities/windowForComponent (:panel contents)) "Add" false)
	from (let [model (SpinnerDateModel.)
		   spinner (JSpinner. model)]
	       spinner)
	to (let [model (SpinnerDateModel.)
		 spinner (JSpinner. model)]
	     spinner)

	combomodels-on-center (atom {})

	onAdd (fn []
		(let [name-values (reduce (fn [result name-combomodel] 
					    (let [the-value (.getSelectedItem (val name-combomodel))]
					      (if (not (= "" the-value))
						(conj result (key name-combomodel) the-value)
						result)))
					  []  @combomodels-on-center)]
		(on-add-to-analysis (.getDate (.getModel from))
			(.getDate (.getModel to))
			name-values
			(:time-series contents)
			(:colors contents)
			(:chart contents)
			(:add-to-table contents)
			(:add-column contents)
			server)))
	
	add (let [add (JButton. "Add")]
	      (.setEnabled add false)
	      add)
	close (JButton. "Close")
	centerPanel (JPanel.)]

    (doto dialog
      (.setDefaultCloseOperation WindowConstants/DISPOSE_ON_CLOSE)
      (.setResizable false))

    (.addActionListener close (proxy [ActionListener] [] (actionPerformed [event] (.dispose dialog))))
    (.addActionListener add (proxy [ActionListener] [] (actionPerformed [event] (onAdd))))
    (let [contentPane (.getContentPane dialog)]
      (. contentPane add centerPanel BorderLayout/CENTER)
      (. contentPane add (doto (JPanel.)
			     (.setLayout (FlowLayout. FlowLayout/CENTER))
			     (.add add)
			     (.add close)) 
	   BorderLayout/SOUTH)


      (let [panel (JPanel.)]      
	(. contentPane add panel BorderLayout/NORTH)
	(doto panel
	  (.setLayout (FlowLayout. FlowLayout/CENTER))
	  (.add (JLabel. "From:"))
	  (.add from)
	  (.add (JLabel. "To:"))
	  (.add to)
	  (.add (let [update (JButton. "Update")
		      onUpdate (fn [] 
				 (let [from (.. from (getModel) (getDate))
				       to (.. to (getModel) (getDate))]
				   (let [names (get-names from to server)]
				     (on-update names combomodels-on-center centerPanel add)
				     (.pack dialog))))]
		  (. update addActionListener (proxy [ActionListener] [] (actionPerformed [_] (onUpdate) )))
		  update)))))
    dialog))



(defn new-analysis-panel [] 
  (let [panel (JPanel.)
;	color-cycle (atom (cycle colors)) 
	time-series (TimeSeriesCollection.)
	chart (ChartFactory/createTimeSeriesChart 
					      nil nil nil 
					      time-series false false false)
	tbl-model (create-table-model
		   (fn [row-num] 
		     (.removeSeries time-series row-num))
		   (fn [row-num color]
		     (.. chart (getPlot) (getRenderer) (setSeriesPaint row-num color))))

	table (JTable.)
	current-row (atom nil)
	popupMenu (doto (JPopupMenu.)
		    (.add ( doto (JMenuItem. "Delete")
			    (.addActionListener (proxy [ActionListener] []
						  (actionPerformed [event]
								   ((:remove-row tbl-model) @current-row)))))))

	popup (fn [event]
		(let [x (.getX event)
		      y (.getY event)
		      row (.rowAtPoint table (Point. x y))]
		  (swap! current-row (fn [_] (.convertRowIndexToModel table row)))
		  (.show popupMenu table x y)))]

    

    (doto panel
      (.setLayout (BorderLayout.))
      (.add (doto (JSplitPane.)
	      (.setOrientation JSplitPane/VERTICAL_SPLIT)
	      (.setName "splitPane")
	      (.setBottomComponent (doto (JScrollPane.)
				   (.setViewportView (doto table
						       (.setDefaultRenderer 
							Color 
							(proxy [TableCellRenderer] []
							  (getTableCellRendererComponent 
							   [table color selected focus row column]
							   (doto (JLabel.)
							     (.setOpaque true)
							     (.setBackground color)
							     ))))
						       (.setModel (:model tbl-model))
						       (.setCellSelectionEnabled false)
						       (.setName "table")
						       (.addMouseListener (proxy [MouseAdapter] []
									    (mousePressed [event]
											  (when (.isPopupTrigger event)
											    (popup event)))
									    (mouseReleased [event]
											   (when (.isPopupTrigger event)
											     (popup event)))))
						       (.setAutoCreateRowSorter true)))))
	      (.setTopComponent (doto (ChartPanel. 
				       (doto chart)))))))
    {:panel panel 
     :add-to-table (:add-row tbl-model)
     :add-column (:add-column tbl-model)
     :chart chart
     :colors (color-cycle)
     :time-series time-series}))

