(ns monitor.analysis
  (:use (clojure stacktrace pprint))
  (:use (monitor commongui tools mem))

  (:import (javax.swing UIManager JFrame JButton JOptionPane JMenuBar JMenu JMenuItem 
			JPanel JScrollPane JSplitPane JTable JCheckBox JLabel Box JDialog JComboBox
			JTextField WindowConstants JSpinner SpinnerDateModel SwingUtilities
			DefaultComboBoxModel GroupLayout GroupLayout$Alignment JPopupMenu
			BorderFactory JSpinner$DateEditor BoxLayout))

  (:import (javax.swing.table TableCellRenderer AbstractTableModel))
  (:import (java.awt Dimension BorderLayout Color FlowLayout Component Point GridLayout 
		     GridBagLayout GridBagConstraints Insets BasicStroke ))
  (:import (java.awt.event WindowAdapter ActionListener MouseAdapter ComponentAdapter ItemEvent ItemListener))
  (:import (java.net Socket UnknownHostException))
  (:import (java.io ObjectInputStream))
  (:import (java.util Calendar Date))
  (:import (java.text SimpleDateFormat))
  (:import (org.jfree.chart.axis NumberAxis))
  (:import (org.jfree.chart ChartFactory ChartPanel JFreeChart ChartUtilities))
  (:import (org.jfree.data.time TimeSeries TimeSeriesCollection TimeSeriesDataItem Millisecond))
  (:import (monitor SplitDateSpinners)))

(defn on-update
  "Returns panel and models"
  [names add-buttons]
  (let [center-panel (doto (JPanel.)
		       (.setLayout  (GridBagLayout.)))
	combos-and-models (loop [combos [] models {} names names]
			    (let [a-name (first names)
				  field-name (name (key a-name))
				  label (JLabel. field-name JLabel/RIGHT)
				  combo-model (DefaultComboBoxModel. (to-array (cons "" (reduce (fn [toadd i]
												  (if-let [a-value ((key a-name) i)]
												    (conj toadd a-value)
												    toadd))
												#{}
												(val a-name)))))
				  combo (doto (JComboBox. combo-model)
					  (.setName field-name))]
			      (doto center-panel
				(.add label (GridBagConstraints. 
					     0 GridBagConstraints/RELATIVE 
					     1 1 
					     0 0 
					     GridBagConstraints/EAST 
					     GridBagConstraints/NONE 
					     (Insets. 1 1 0 4) 
					     0 0 ))
				(.add combo (GridBagConstraints. 
					     1 GridBagConstraints/RELATIVE 
					     1 1 
					     0 0 
					     GridBagConstraints/WEST 
					     GridBagConstraints/NONE 
					     (Insets. 0 0 0 0) 
					     0 0 )))
			      (if-let [names (next names)]
				(recur (conj combos combo) (assoc models (key a-name) combo-model) names)
				[(conj combos combo) (assoc models (key a-name) combo-model)])))
	combo-action (create-comboaction (second combos-and-models) (first combos-and-models) add-buttons names)]
    (dorun (map (fn [combo] (.addActionListener combo combo-action))
		(first combos-and-models)))
    [center-panel (second combos-and-models)]
    ))
		      
(defn- time-serie-to-sortedmap [timeserie]
  (reduce (fn [r i] 
	  (assoc r (Date. (.getFirstMillisecond (.getPeriod i ))) (.getValue i))) 
	  (sorted-map) (.getItems timeserie))
)

			
(defn on-add-to-analysis [from to shift name-values all-names func-string granularity-string graph colors chart add-to-table add-column statusbar server]
  (let [stop-chart-notify (fn stop-chart-notify []  (.setNotify chart false)
			    (dorun (map (fn [timeseries] (.setNotify timeseries false)) (. graph getSeries))))
	start-chart-notify (fn start-chart-notify [] (dorun (map (fn [timeseries] (.setNotify timeseries true)) (. graph getSeries)))
			     (.setNotify chart true))

	shift-time (if (= 0 shift)
		     (fn [a] a)
		     (fn [a] (with-keys-shifted a (fn [d] d) (fn [#^Date d] (Date. (+ (.getTime d) shift))))))
		     
	create-new-time-serie (fn [data-key identifier]
				(let [serie (TimeSeries. identifier)
				      color (colors)]
				  (.setNotify serie false)
				  (. graph addSeries serie)
				  
				  (.. chart 
				      (getPlot) (getRenderer) (setSeriesPaint (dec (count (.getSeries graph)))
									      color))
				  
				  (add-to-table data-key color identifier)
				  (dorun (map (fn [i] (add-column i)) (keys data-key))) 
				  serie))
	updatechart (fn [data] (SwingUtilities/invokeLater 
				(fn []
				  
				  (stop-chart-notify)
				  (dorun (map (fn [data]
						(let [data-key (let [dk (assoc (key data) :type func-string :granularity granularity-string)]
								 (if (= 0 shift)
								   dk
								   (let [df (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")]
								     (assoc dk :shifted (str (.format df from) " to " (.format df
															       (Date. (+ (.getTime from) shift))
															       ))))))
						      make-double-values (fn [e] (into (sorted-map) (map #(first {(key %) (double (val %))}) e)))
						      data-values (make-double-values (val data))
						      identifier (str data-key)
						      time-serie (if-let [serie (. graph getSeries identifier)]
								   serie
								   (create-new-time-serie data-key identifier))
						      data-from-serie (time-serie-to-sortedmap time-serie)
						      data-with-new-data (merge data-from-serie data-values)
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
						  (.addAndOrUpdate time-serie new-serie)))
					;	  (.add time-serie new-serie)))
					      data))
				  (start-chart-notify))))]
    
	   (future  	     
	     (if (= "Raw" func-string)
	       (let [days (full-days-between from to)
		     current (atom 0)]
		 (dorun (map (fn [ e ]
			       (swap! current inc) 
			       (.setText statusbar (str "Retrieveing " @current " of " (count days)))
			       (updatechart (shift-time (get-data (first e)
						      (second e)
						      name-values
						      func-string
						      granularity-string
						      server))))
			     days)))
		 
		 
	       (let [current (atom 0)
		     names-of-interest (let [sel-as-map (apply assoc {} name-values)]
					 (reduce (fn [c a] 
						   (let [s (select-keys a (keys sel-as-map))]
						     (if (= s sel-as-map)
						       (conj c a)
						       c)))
						 [] all-names))]
		 (dorun (map (fn [e]
			(swap! current inc)
			(.setText statusbar (str "Retrieveing " @current " of " (count names-of-interest)))
			(updatechart (shift-time (get-data from
					       to
					       (reduce (fn [v e]
							 (conj v (key e) (val e))) [] e)
					       func-string
					       granularity-string
					       server))))
		      
		      names-of-interest))))

	       (.setText statusbar " ")
)))



(defn analysis-add-dialog [contents server]
  (let [dialog (JDialog. (SwingUtilities/windowForComponent (:panel contents)) "Add" false)
	from-model (let [d @(:from contents)
			   
			c (Calendar/getInstance)
		     
			startd (.getTime (doto c
					   (.setTime d)
					   (.add Calendar/YEAR -10)))
			endd (.getTime (doto c
					 (.setTime d)
					 (.add Calendar/YEAR +1)))]
		     (SplitDateSpinners. d endd startd ))		
	to-model (let [d @(:to contents)
			 
			c (Calendar/getInstance)
		     
			startd (.getTime (doto c
					   (.setTime d)
					   (.add Calendar/YEAR -10)))
			endd (.getTime (doto c
					 (.setTime d)
					 (.add Calendar/YEAR +1)))]
		   (SplitDateSpinners. d endd startd ))
	shift-model (let [d @(:from contents)
			   
			c (Calendar/getInstance)
		     
			startd (.getTime (doto c
					   (.setTime d)
					   (.add Calendar/YEAR -10)))
			endd (.getTime (doto c
					 (.setTime d)
					 (.add Calendar/YEAR +1)))]
		     (SplitDateSpinners. d endd startd ))		

	granularity-combo (doto (JComboBox. (to-array ["All Data" "Minute" "Hour" "Day"]))
			    (.setEnabled false))

	func-combo (let [c (JComboBox. (to-array ["Raw" "Average/Minute" "Average/Hour" "Average/Day"
						     "Mean/Minute" "Mean/Hour" "Mean/Day"
						     "Min/Minute" "Min/Hour" "Min/Day"
						     "Max/Minute" "Max/Hour" "Max/Day"
						     "Change/Second"
						     "Change/Minute"
						     "Change/Hour"]))]
		     (.addActionListener c (proxy [ActionListener] []
			      (actionPerformed [_]
					       (let [fun (.getSelectedItem (.getModel c ))
						     gran-model (.getModel granularity-combo)]
						 (if (= fun "Raw")
						   (.setEnabled granularity-combo false)
						   (do (.setEnabled granularity-combo true)
						   (cond
						    (some #{fun} ["Average/Minute" 
								  "Mean/Minute" 
								  "Min/Minute"
								  "Max/Minute"])
						    (.setModel granularity-combo (DefaultComboBoxModel. (to-array ["All Data" "Minute"])))
						    (some #{fun} ["Average/Hour"
								  "Mean/Hour" 
								  "Min/Hour" 
								  "Max/Hour"])
						    (.setModel granularity-combo (DefaultComboBoxModel. (to-array ["All Data" "Minute" "Hour"])))
						    (some #{fun} ["Average/Day"
								  "Mean/Day" 
								  "Min/Day" 
								  "Max/Day"
								  "Change/Second"
								  "Change/Minute"
								  "Change/Hour"])
						    (.setModel granularity-combo (DefaultComboBoxModel. (to-array ["All Data" "Minute" "Hour" "Day"])))
						    )))))))
		     c)
		     
	all-names (atom [])
	center-panel (atom (JPanel.))	
	combomodels-on-center (atom {})
	shift-check (doto (JCheckBox. "Shift start to:")
		      (.addItemListener (proxy [ItemListener] []
				    (itemStateChanged [e] (.enable shift-model (= ItemEvent/SELECTED (.getStateChange e)))))))
		      
	onAdd (fn [contents]
		(reset! (:from contents) (.getDate from-model))
		(reset! (:to contents) (.getDate to-model))
		(let [name-values (reduce (fn [result name-combomodel] 
					    (let [the-value (.getSelectedItem (val name-combomodel))]
					      (if (not (= "" the-value))
						(conj result (key name-combomodel) the-value)
						result)))
					  []  @combomodels-on-center)
		      from (.getDate from-model)
		      to (.getDate to-model)

		      resulting-number-of-graphs (fn [panel names]
					      (let [selection (reduce (fn [r e]
									(assoc r (keyword (.getName e)) (.getSelectedItem e)))
								      {}
								      (filter #(not (= "" (.getSelectedItem %)))
									      (filter #(.isEnabled %)
										      (filter #(instance? JComboBox %)
											      (vec (.getComponents panel))))))]
						(count (filter #(= (select-keys % (keys selection)) selection) names))))
		      are-you-sure (fn are-you-sure [from to func-string granularity-string]
				     (if (< to from)
				       (are-you-sure to from)
				       (if (cond
					    (and (< (* 1000 60 60 24) (- to from))
						 (= "Raw" func-string))
					    true
					    (and (< (* 1000 60 60 24 2) (- to from))
						 (some #{granularity-string} ["Minute" "All Data"]))
					    true
					    (and (< (* 1000 60 60 24 7) (- to from))
						 (some #{granularity-string} ["Hour" "Minute" "All Data"]))
					    true
					    (and (< (* 1000 60 60 24 50) (- to from))
						 (some #{granularity-string} ["Day" "Hour" "Minute" "All Data"]))
					    true
					    (< 10  (resulting-number-of-graphs @center-panel @all-names))
					    true
					    true false)
					 (= JOptionPane/YES_OPTION (JOptionPane/showConfirmDialog dialog "Are you sure, this appear to be a lot of data?" "Are you sure?" JOptionPane/YES_NO_OPTION))
					 true)
				       ))]
		  ;			
		  (when (are-you-sure (.getTime from) (.getTime to) (.getSelectedItem func-combo) (.getSelectedItem granularity-combo))
		    (let [shift (if (.isSelected (.getModel shift-check))
				  (- (.getTime (.getDate shift-model)) (.getTime from))
				  0)]
		    (on-add-to-analysis from
					to
					shift
					name-values
					@all-names
					(.getSelectedItem func-combo)
					(.getSelectedItem granularity-combo)
					(:time-series contents)
					(:colors contents)
					(:chart contents)
					(:add-to-table contents)
					(:add-column contents)
					(:status-label contents)
					server)))))
	
	add (let [add (JButton. "Add")]
	      (.setEnabled add false)
	      add)
	add-new-window (let [add (JButton. "Add New")]
			 (.setEnabled add false)
			 add)
	close (JButton. "Close")

	
    
	]
    

    (doto dialog
      (.setDefaultCloseOperation WindowConstants/DISPOSE_ON_CLOSE)
      (.setResizable false))
    
    (.addActionListener close (proxy [ActionListener] [] (actionPerformed [event] (.dispose dialog))))
    (.addActionListener add (proxy [ActionListener] [] (actionPerformed [event] (onAdd contents))))
    (.addActionListener add-new-window (proxy [ActionListener] []
					 (actionPerformed [event]
							  (onAdd (@new-window-fn true))
							  )))
    
    (let [contentPane (.getContentPane dialog)]
      (. contentPane add @center-panel BorderLayout/CENTER)
      (. contentPane add (doto (JPanel.)
			   (.setLayout (FlowLayout. FlowLayout/CENTER))
			   (.add func-combo)
			   (.add granularity-combo)
			   (.add add)
			   (.add add-new-window)
			   (.add close)) 
	   BorderLayout/SOUTH)


      (let [panel (JPanel.)
	    from-panel (JPanel.)
	    to-panel (JPanel.)
	    update-panel (JPanel.)
	    shift-panel (JPanel.)
	    time-panel (JPanel.)]
	
	(. panel setLayout (BoxLayout. panel BoxLayout/PAGE_AXIS))
	(. contentPane add panel BorderLayout/NORTH)
	(doto panel
	  (.add from-panel)
	  (.add to-panel )
	  (.add shift-panel)
	  (.add update-panel))
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
	(doto shift-panel 
      	  (.setLayout (FlowLayout. FlowLayout/TRAILING ))
	  (.add shift-check)
	  (.add (.yearSpinner shift-model))
	  (.add (.monthSpinner shift-model))
	  (.add (.daySpinner shift-model))
	  (.add (JLabel. "time:"))
	  (.add (.hourSpinner shift-model))
	  (.add (.minuteSpinner shift-model))
	  (.add (.secondSpinner shift-model)))
	(.enable shift-model false)
	  
	(.setLayout update-panel (FlowLayout. FlowLayout/TRAILING ))
	  (.add update-panel (let [update (JButton. "Update")
				   onUpdate (fn [] 
					      (let [from (. from-model getDate)
						    to (. to-model getDate)]
						(let [names (get-names from to server)]
						  (swap! all-names (fn [_]
								     (reduce (fn [r e]
					;e är lista
									       (reduce (fn [r e]
											 (conj r e)) r e)
									       ) #{} (vals names)) 
								     ))
						  
						  (let [panel-and-models (on-update names [add add-new-window])
							panel (first panel-and-models)
							content-pane (.getContentPane dialog)]
						    (.remove content-pane @center-panel)
						    (reset! center-panel panel)
						    (reset! combomodels-on-center (second panel-and-models))
						    (.add content-pane panel BorderLayout/CENTER)
						    )
						  (.pack dialog))))]
		  (. update addActionListener (proxy [ActionListener] [] (actionPerformed [_] (onUpdate) )))
		  update))))
    dialog))



(defn new-analysis-panel []

  (let [status-label (JLabel. " ")
	panel (JPanel.)
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
		  (.show popupMenu table x y)))
	highlighted (atom nil)
	mouse-table-adapter (proxy [MouseAdapter] []
			      (mousePressed [event]
					    (when (.isPopupTrigger event)
					      (popup event)))
			      (mouseReleased [event]
					     (when (.isPopupTrigger event)
					       (popup event)))
			      (mouseMoved [event]
					  (let [x (.getX event)
						y (.getY event)
						row (.convertRowIndexToModel table (.rowAtPoint table (Point. x y)))
						renderer (.. chart (getPlot) (getRenderer))
						stroke (.getSeriesStroke renderer row)]
					    (when-let [hl @highlighted]
					      (when-not (= (first hl) row)
						(.setSeriesStroke renderer (first hl) (second hl))
						(reset! highlighted nil)
						))
					    (when-not @highlighted
					      (.setSeriesStroke renderer row (BasicStroke. 2.0)) 
					      (reset! highlighted [row stroke]))))
			      
			      (mouseExited [event]
					   (when-let [hl @highlighted]
					     (let [renderer (.. chart (getPlot) (getRenderer))]
					       (.setSeriesStroke renderer (first hl) (second hl))
											       (reset! highlighted nil)) 
					     )))
	]


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
      (.add status-label BorderLayout/SOUTH)
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
						      
						       (.addMouseListener mouse-table-adapter)
						       (.addMouseMotionListener mouse-table-adapter)
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
									     ))))))) BorderLayout/CENTER))
				 
 ;(ChartUtilities/applyCurrentTheme chart)				  
    {:panel panel
     :status-label status-label
     :add-to-table (:add-row tbl-model)
     :add-column (:add-column tbl-model)
     :chart chart
     :colors (color-cycle)
     :time-series time-series
     :from (atom (Date. (- (System/currentTimeMillis) (* 1000 60 60))))
     :to (atom (Date.))
     :name "Monitor - Analysis Window"
     }))

