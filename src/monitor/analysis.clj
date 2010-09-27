(ns monitor.analysis
  (:use (clojure stacktrace))
  (:use (monitor commongui tools))
  (:import (javax.swing UIManager JFrame JButton JOptionPane JMenuBar JMenu JMenuItem 
			JPanel JScrollPane JSplitPane JTable JLabel Box JDialog JComboBox
			JTextField WindowConstants JSpinner SpinnerDateModel SwingUtilities
			DefaultComboBoxModel GroupLayout GroupLayout$Alignment JPopupMenu
			BorderFactory JSpinner$DateEditor))

  (:import (javax.swing.table TableCellRenderer AbstractTableModel))
  (:import (java.awt Dimension BorderLayout Color FlowLayout Component Point GridLayout 
		     GridBagLayout GridBagConstraints Insets ))
  (:import (java.awt.event WindowAdapter ActionListener MouseAdapter ComponentAdapter))
  (:import (java.net Socket UnknownHostException))
  (:import (java.io ObjectInputStream))
  (:import (java.util Calendar Date))
  (:import (java.text SimpleDateFormat))
  (:import (org.jfree.chart.axis NumberAxis))
  (:import (org.jfree.chart ChartFactory ChartPanel JFreeChart ChartUtilities))
  (:import (org.jfree.data.time TimeSeries TimeSeriesCollection TimeSeriesDataItem Millisecond))
  (:import (monitor SplitDateSpinners)))


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

(defn- time-serie-to-sortedmap [timeserie]
  (reduce (fn [r i] 
	  (assoc r (Date. (.getFirstMillisecond (.getPeriod i ))) (.getValue i))) 
	  (sorted-map) (.getItems timeserie))
)

			
(defn on-add-to-analysis [from to name-values func-string granularity-string graph colors chart add-to-table add-column server]
					;  (.start (Thread.

	   (future  
	     
	     (let [result (reduce (fn [c e]
				    (merge c (get-data (first e)
						       (second e)
						       name-values
						       func-string
						       granularity-string
						       server)))
				  {}
				  (full-days-between from to))]
		      (SwingUtilities/invokeLater 
		       (fn [] 
			 (.setNotify chart false)
			 (dorun (map (fn [timeseries] (.setNotify timeseries false)) (. graph getSeries)))
			 (dorun (map (fn [data]
				       (let [identifier (str func-string granularity-string (key data))
					     time-serie 
					     (if-let 
						 [serie 
						  (. graph getSeries identifier)]
					       serie
					       (let [serie (TimeSeries. identifier)
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
						 
						 (add-to-table (key data) color identifier)
						 (dorun (map (fn [i] (add-column i)) (keys (key data)))) 
						 serie))]
					 (let [data-from-serie (time-serie-to-sortedmap time-serie)
					       data-with-new-data (merge data-from-serie (val data))
					       nan-distance (condp = granularity-string
								"All Data" (* 30 1000)
							      "Minute" (* 2 60 1000)
							      "Hour" (* 2 60 60 1000)
							      "Day" (* 2 24 60 60 1000))
					       data-with-nans (with-nans data-with-new-data nan-distance)
					       temp-serie (doto (TimeSeries. "") (.setNotify false))
					       new-serie (reduce (fn [r i]
								   (.add r  (TimeSeriesDataItem. 
									(Millisecond. (key i)) 
									(val i)))
								   r)
								 temp-serie data-with-nans)]
					   (.clear time-serie)
					   (.addAndOrUpdate time-serie new-serie))))
				     result))
			 (dorun (map (fn [timeseries] (.setNotify timeseries true)) (. graph getSeries)))
			 (.setNotify chart true))))
		   ))
;)


(defn analysis-add-dialog [contents server]
  (let [dialog (JDialog. (SwingUtilities/windowForComponent (:panel contents)) "Add" false)
	from-model (let [d (Date.)
			c (Calendar/getInstance)
		     
			startd (.getTime (doto c
					   (.setTime d)
					   (.add Calendar/YEAR -10)))
			endd (.getTime (doto c
					 (.setTime d)
					 (.add Calendar/YEAR +1)))]
		     (SplitDateSpinners. d endd startd ))		
	to-model (let [d (Date.)
			c (Calendar/getInstance)
		     
			startd (.getTime (doto c
					   (.setTime d)
					   (.add Calendar/YEAR -10)))
			endd (.getTime (doto c
					 (.setTime d)
					 (.add Calendar/YEAR +1)))]
		     (SplitDateSpinners. d endd startd ))		

	func-combo (JComboBox. (to-array ["Raw" "Average/Minute" "Average/Hour" "Average/Day"
						     "Mean/Minute" "Mean/Hour" "Mean/Day"
						     "Min/Minute" "Min/Hour" "Min/Day"
						     "Max/Minute" "Max/Hour" "Max/Day"
						     "Change/Second"
						     "Change/Minute"
						     "Change/Hour"]))
	

	granularity-combo (JComboBox. (to-array ["All Data" "Minute" "Hour" "Day"]))
	combomodels-on-center (atom {})

	onAdd (fn []
		(let [name-values (reduce (fn [result name-combomodel] 
					    (let [the-value (.getSelectedItem (val name-combomodel))]
					      (if (not (= "" the-value))
						(conj result (key name-combomodel) the-value)
						result)))
					  []  @combomodels-on-center)]
		(on-add-to-analysis (.getDate from-model)
				    (.getDate to-model)
				    name-values
				    (.getSelectedItem func-combo)
				    (.getSelectedItem granularity-combo)
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
			   (.add func-combo)
			   (.add granularity-combo)
			   (.add add)
			   (.add close)) 
	   BorderLayout/SOUTH)


      (let [panel (JPanel.)
	    from-panel (JPanel.)
	    to-panel (JPanel.)
	    update-panel (JPanel.)]
	(. panel setLayout (BorderLayout.))
	(. contentPane add panel BorderLayout/NORTH)
	(doto panel
	  (. add from-panel BorderLayout/NORTH)
	  (. add to-panel BorderLayout/CENTER)
	  (. add update-panel BorderLayout/SOUTH))
	(doto from-panel
	  (.setLayout (FlowLayout. FlowLayout/TRAILING ))
	  (.add (JLabel. "From date:"))
	  (.add (.yearSpinner from-model))
	  (.add (.monthSpinner from-model))
	  (.add (.daySpinner from-model))
	  (.add (JLabel. "time:"))
	  (.add (.hourSpinner from-model))
	  (.add (.minuteSpinner from-model))
	  (.add (.secondSpinner from-model)))

	(doto to-panel
	  (.setLayout (FlowLayout. FlowLayout/TRAILING ))
	  (.add (JLabel. "To date:"))
	  (.add (.yearSpinner to-model))
	  (.add (.monthSpinner to-model))
	  (.add (.daySpinner to-model))
	  (.add (JLabel. "time:"))
	  (.add (.hourSpinner to-model))
	  (.add (.minuteSpinner to-model))
	  (.add (.secondSpinner to-model)))
	(.setLayout update-panel (FlowLayout. FlowLayout/TRAILING ))
	  (.add update-panel (let [update (JButton. "Update")
				   onUpdate (fn [] 
					      (let [from (. from-model getDate)
						    to (. to-model getDate)]
						(let [names (get-names from to server)]
						  (on-update names combomodels-on-center centerPanel add)
						  (.pack dialog))))]
		  (. update addActionListener (proxy [ActionListener] [] (actionPerformed [_] (onUpdate) )))
		  update))))
    dialog))



(defn new-analysis-panel [] 
  (let [panel (JPanel.)
	time-series (TimeSeriesCollection.)
;	right-series (TimeSeriesCollection.)
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


    (.setDateFormatOverride (.getDomainAxis (.getPlot chart)) (SimpleDateFormat. "yy-MM-dd HH:mm:ss"))
   ;(let [right-axis (NumberAxis.)
;	  plot (.getPlot chart)]
 ;     (.setRangeAxis plot 1 right-axis)
  ;    (.setDataset plot 1 right-series)
   ;   )


    (doto (.getPlot chart)
      (.setBackgroundPaint Color/white)
      (.setRangeGridlinePaint Color/gray)
      (.setOutlineVisible false))
    
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
							  ; (doto (JLabel. "<")
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
				       (doto chart))
				  (.addComponentListener (proxy [ComponentAdapter] []
							   (componentResized [e] 
									     (let [c (.getComponent e)
										   size (.getSize c)]
									     ;(println (str "sized " c))
									     
									     (SwingUtilities/invokeLater 
									      (fn []
										(.setMinimumDrawHeight c (.getHeight size))
										(.setMinimumDrawWidth c (.getWidth size))
										(.setMaximumDrawHeight c (.getHeight size))
										(.setMaximumDrawWidth c  (.getWidth size))
										(.fireChartChanged (.getChart c))
										))
									     )))))))))
				 
 ;(ChartUtilities/applyCurrentTheme chart)				  
    {:panel panel 
     :add-to-table (:add-row tbl-model)
     :add-column (:add-column tbl-model)
     :chart chart
     :colors (color-cycle)
     :time-series time-series
     :name "Monitor - Analysis Window"
     ;:right-series right-series
     }))

