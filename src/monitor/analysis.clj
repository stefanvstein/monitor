(ns monitor.analysis
  (:use (clojure stacktrace pprint))
  (:use (monitor commongui tools mem))

  (:import (javax.swing UIManager JFrame JButton JOptionPane JMenuBar JMenu JMenuItem 
			JPanel JScrollPane JSplitPane JTable JCheckBox JLabel Box JDialog JComboBox
			JTextField WindowConstants JSpinner SpinnerDateModel SwingUtilities
			DefaultComboBoxModel GroupLayout BoxLayout GroupLayout$Alignment JPopupMenu
			BorderFactory JSpinner$DateEditor  DefaultCellEditor Box))

  (:import (javax.swing.table TableCellRenderer AbstractTableModel))
  (:import (java.awt Dimension BorderLayout Color FlowLayout Component Point GridLayout 
		     GridBagLayout GridBagConstraints Insets BasicStroke ))
  (:import (java.awt.event WindowAdapter ActionListener MouseAdapter ComponentAdapter ItemEvent ItemListener))
  (:import (java.net Socket UnknownHostException))
  (:import (java.io ObjectInputStream))
  (:import (java.util Calendar Date))
  (:import (java.text SimpleDateFormat))
  (:import (java.beans PropertyChangeListener))
  (:import (org.jfree.chart.axis NumberAxis))
  (:import (org.jfree.chart.event ChartProgressEvent ChartProgressListener))
  (:import (org.jfree.chart ChartFactory ChartPanel JFreeChart ChartUtilities))
  (:import (org.jfree.data.time TimeSeries TimeSeriesCollection TimeSeriesDataItem Millisecond))
  (:import (monitor SplitDateSpinners)))

(defn- date-order [d1 d2]
  (if (< (.getTime d1) (.getTime d2))
    [d1 d2]
    [d2 d1]))

(defn on-update
  "Returns panel and models"
  [names add-buttons]
  (let [center-panel (doto (JPanel.)
		       (.setLayout  (GridBagLayout.)))
	combos-and-models (when (seq names)
			    (loop [combos [] models {} names names]
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
				  [(conj combos combo) (assoc models (key a-name) combo-model)]))))
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

(defn- index-by-name [time-series-collection name]
  (let [n (.getSeriesCount time-series-collection)]
    (loop [i 0]
      (if (< i n)
	(if (= name (.getSeriesKey time-series-collection i))
	  i
      	  (recur (inc i)))
	nil))))
  

					;(defn on-add-to-analysis [from to shift name-values all-names func-string graph colors chart add-to-table add-column statusbar server]
(defn on-add-to-analysis [from to shift name-values all-names func-string
			  {^TimeSeriesCollection graph :time-series
			   colors :colors
			   ^JFreeChart chart  :chart
			   add-to-table :add-to-table
			   add-column :add-column
			   statusbar :status-label
			   data-queue :transfer-queue
			   num-to-render :num-to-render
			   num-rendered :num-rendered}
			  server]
  (let [stop-chart-notify (fn stop-chart-notify []  (.setNotify chart false)
					;(dorun (map (fn [timeseries] (.setNotify timeseries false)) (. graph getSeries)))
			    )
	start-chart-notify (fn start-chart-notify []
			     ;(dorun (map (fn [timeseries] (.setNotify timeseries true)) (. graph getSeries)))
			     (.setNotify chart true))

	shift-time (if (= 0 shift)
		     (fn [a] a)
		     (fn [a] (with-keys-shifted a (fn [d] d) (fn [#^Date d] (Date. (long (+ (.getTime d) shift)))))))
		     
	create-new-time-serie (fn [data-key identifier]
				(let [serie (TimeSeries. identifier)
				      color (colors)]
				  (. serie setNotify false)
				  (. graph addSeries serie)
				  (let [visible-fn (fn ([_]
							  
							  (if-let [index (index-by-name graph identifier)]
							    (.. chart (getPlot) (getRenderer) (getItemVisible index 0 ))
							    false))
						     ([_ visible]
							(when-let [index (index-by-name graph identifier)]
							  (.. chart (getPlot) (getRenderer) (setSeriesVisible index visible))
							  )))]
				    (.. chart 
					(getPlot) (getRenderer) (setSeriesPaint (dec (count (.getSeries graph)))
										color))
				    
				    (add-to-table data-key color identifier visible-fn))
				  (dorun (map (fn [i] (add-column i)) (keys data-key))) 
				  serie))
	updatechart (fn [data]
		      (swap! data-queue conj data)
		      (SwingUtilities/invokeLater
		       (fn []
			 (let [datas (into [] (take 5 @data-queue))]
			   (when-not (zero? (count datas))

			     (swap! data-queue (fn [dq] (vec (drop (count datas) dq))))
			     (let [u (swap! num-rendered (fn [d] (+ d (count datas) )))
				   num @num-to-render]
			     (println "Rendering" u "of" num)
			       (.setText statusbar (str "Rendering " u " of " num)))
			     
				  (try
				    (stop-chart-notify)
				    (doseq [data datas]
				  (dorun (map (fn [data]
						(let [data-key (let [dk (assoc (key data) :type func-string)]
								 (if (= 0 shift)
								   dk
								   (let [df (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")]
								     (assoc dk :shifted (str (.format df from) " to " (.format df
															       (Date. (long (+ (.getTime ^Date from) shift)))
															       ))))))
						      make-double-values (fn [e] (into (sorted-map) (map #(first {(key %) (double (val %))}) e)))
						      data-values (make-double-values (val data))
						      identifier (str data-key)
						      ^TimeSeries time-serie (if-let [serie (. graph getSeries identifier)]
								   serie
								   (create-new-time-serie data-key identifier))
						      data-from-serie (time-serie-to-sortedmap time-serie)
						      data-with-new-data (merge data-from-serie data-values)
						      nan-distance (condp = func-string
								       "Raw" (* 30 1000)
								       "Change/Second" (* 30 1000)
								       "Average/Minute" (* 2 60 1000)
								       "Mean/Minute" (* 2 60 1000)
								       "Min/Minute" (* 2 60 1000)
								       "Max/Minute" (* 2 60 1000)
								       "Change/Minute" (* 2 60 1000)
								       "Average/Hour" (* 2 60 60 1000)
								       "Mean/Hour" (* 2 60 60 1000)
								       "Min/Hour" (* 2 60 60 1000)
								       "Max/Hour" (* 2 60 60 1000)
								       "Change/Hour" (* 2 60 60 1000)
								       "Average/Day" (* 2 24 60 60 1000)
								       "Mean/Day" (* 2 24 60 60 1000)
								       "Min/Day" (* 2 24 60 60 1000)
								       "Max/Day" (* 2 24 60 60 1000))
						      data-with-nans (with-nans data-with-new-data nan-distance)
						      reduce-samples (fn reduce-samples [data] 
								       (if (next data) 
									 (loop [s (next data), result data, previous (first data), was-equal false]
									   (let [current (first s)]
									     (if-let [following (next s)]
									       (if (= (val previous) (val current))
										 (if was-equal
										   (recur following (dissoc result (key previous)) current true)
										   (recur following result current true))
										 (recur following result current false))
									       (if (= (val previous) (val current)) 
										 (if was-equal
										   (dissoc result (key previous))
										   result)
										 result))))
									 data))
						      reduced-samples (let [r (reduce-samples data-with-nans)]
;									(println (count data-with-nans) "->" (count r))
									r)
						      temp-serie (doto (TimeSeries. "") (.setNotify false))
						      new-serie (reduce (fn [^TimeSeries r i]
									  (.add r  (TimeSeriesDataItem. 
										    (Millisecond. (key i)) 
										    ^Double (val i)))
									  r)
									temp-serie reduced-samples)]
						 
						    
						  (.clear time-serie)
						  (.addAndOrUpdate time-serie new-serie)))
					      data)))
				  (catch Exception e
				    (.printStackTrace e))
				  (finally   (start-chart-notify))))))))]
    
    (future
      (try
	(if (= "Raw" func-string)
	  (let [days (full-days-between from to)]
	    (swap! num-to-render (fn [d] (+ d (count days))))
	    (dorun (map (fn [ e ]
			  
			  
			  (updatechart (shift-time (get-data (first e)
							     (second e)
							     name-values
							     func-string
							     server))))
			days))
		 
	    (SwingUtilities/invokeLater (fn []
					  (when (= @num-rendered @num-to-render)
					    (reset! num-rendered 0)
					    (reset! num-to-render 0)
					    (.setText statusbar " ")))))
		 
	  (let [names-of-interest (let [sel-as-map (apply assoc {} name-values)]
				    (reduce (fn [c a] 
					      (let [s (select-keys a (keys sel-as-map))]
						(if (= s sel-as-map)
						  (conj c a)
						  c)))
					    [] all-names))]
	    (swap! num-to-render (fn [d] (+ d (count names-of-interest))))
	    (dorun (map (fn [e]
			  (updatechart (shift-time (get-data from
							     to
							     (reduce (fn [v e]
								       (conj v (key e) (val e))) [] e)
							     func-string
							     server))))
		      
			names-of-interest))
	    (SwingUtilities/invokeLater (fn []
					  (when (= @num-rendered @num-to-render)
					    (reset! num-rendered 0)
					    (reset! num-to-render 0)
					    (.setText statusbar " "))))))
	    
	    
	       
	    (catch Exception e
	      (.printStackTrace e))))))



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

	func-combo (JComboBox. (to-array ["Raw" "Average/Minute" "Average/Hour" "Average/Day"
						     "Mean/Minute" "Mean/Hour" "Mean/Day"
						     "Min/Minute" "Min/Hour" "Min/Day"
						     "Max/Minute" "Max/Hour" "Max/Day"
						     "Change/Second"
						     "Change/Minute"
						     "Change/Hour"]))
	all-names (atom [])
	center-panel (atom (JPanel.))	
	combomodels-on-center (atom {})
	shift-check (doto (JCheckBox. "Shift start to:")
		      (.addItemListener (proxy [ItemListener] []
				    (itemStateChanged [e] (.enable shift-model (= ItemEvent/SELECTED (.getStateChange e)))))))


	
	onAdd (fn [contents]
		(let [dates-in-order (date-order (.getDate from-model) (.getDate to-model))]
		  (reset! (:from contents) (first dates-in-order))
		  (reset! (:to contents) (second dates-in-order))
		  (let [name-values (reduce (fn [result name-combomodel] 
					      (let [the-value (.getSelectedItem (val name-combomodel))]
						(if (not (= "" the-value))
						  (conj result (key name-combomodel) the-value)
						  result)))
					    []  @combomodels-on-center)
			
			from (first dates-in-order)
			to (second dates-in-order)
			
			resulting-number-of-graphs (fn [panel names]
						     (let [selection (reduce (fn [r e]
									       (assoc r (keyword (.getName e)) (.getSelectedItem e)))
									     {}
									     (filter #(not (= "" (.getSelectedItem %)))
										     (filter #(.isEnabled %)
											     (filter #(instance? JComboBox %)
												     (vec (.getComponents panel))))))]
						       (count (filter #(= (select-keys % (keys selection)) selection) names))))
			are-you-sure (fn are-you-sure [from to func-string]
				       (if (< to from)
					 (are-you-sure to from func-string)
					 (if (cond
					      (and (< (* 1000 60 60 26) (- to from))
						   (= "Raw" func-string))
					      true
					      (and (< (* 1000 60 60 24 2) (- to from))
						   (some #{func-string} ["Average/Minute" "Mean/Minute" "Max/Minute" "Min/Minute" "Change/Minute" "Change/Second" "Raw"]))
					      true
					      (and (< (* 1000 60 60 24 7) (- to from))
						   (some #{func-string} ["Average/Hour" "Mean/Hour" "Max/Hour" "Min/Hour" "Change/Hour" "Average/Minute" "Mean/Minute" "Max/Minute" "Min/Minute" "Change/Minute" "Change/Second" "Change/Hour" "Raw"]))
					      true
					      (< (* 1000 60 60 24 50) (- to from))
					      true
					      (< 10  (resulting-number-of-graphs @center-panel @all-names))
					      true
					      true false)
					   (= JOptionPane/YES_OPTION (JOptionPane/showConfirmDialog dialog "Are you sure, this appear to be a lot of data?" "Are you sure?" JOptionPane/YES_NO_OPTION))
					   true)
					 ))
					;
			fun (.getSelectedItem func-combo)
			shift (if (.isSelected (.getModel shift-check))
				    (- (.getTime (.getDate shift-model)) (.getTime from))
				    0)]
		    (when (are-you-sure (.getTime from) (.getTime to) fun)
			(on-add-to-analysis from
					    to
					    shift
					    name-values
					    @all-names
					    fun
					    contents
					    server)
			))))
		
	add (let [add (JButton. "Add")]
	      (.setEnabled add false)
	      add)
	add-new-window (let [add (JButton. "Add New")]
			 (.setEnabled add false)
			 add)
	close (JButton. "Close")]
	
	
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
						(let [dates-in-order (date-order (.getDate from-model) (.getDate to-model))
						      from (first dates-in-order)
						      to (second dates-in-order)]
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
				 update)))
	dialog)))



(defn new-analysis-panel []

  (let [status-label (JLabel. " ")
	connection-label (JLabel. " ")
			   	
			  
	status-panel (let [p (JPanel.)
			   layout (BoxLayout. p BoxLayout/LINE_AXIS)]
		       
		       (doto p
			 (.setLayout layout)
			 (.add (doto connection-label
			       (.addPropertyChangeListener (proxy [PropertyChangeListener] []
							     (propertyChange [e] (if (= "text" (.getPropertyName e))
										   (.invalidateLayout layout p)))))))
			 (.add (Box/createRigidArea (Dimension. 5 0)))
			 (.add (doto status-label
				 (.addPropertyChangeListener (proxy [PropertyChangeListener] []
							       (propertyChange [e] (if (= "text" (.getPropertyName e))
										     (.invalidateLayout layout p)))))))))

	panel (JPanel.)
	time-series (TimeSeriesCollection.)
;	right-series (TimeSeriesCollection.)
	chart (doto (ChartFactory/createTimeSeriesChart 
					      nil nil nil 
					      time-series false false false))
		
	


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

		(.addProgressListener chart (proxy [ChartProgressListener] []
					(chartProgress [e]
						       (when (= (.getType e) ChartProgressEvent/DRAWING_FINISHED)
							 (when-let [date (let [l (long (.getDomainCrosshairValue (.getPlot chart)))]
									   (if (zero? l)
									     nil
									     (Date. l)))]
							   (.setText status-label (.format (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss") date))
							   (doseq [s (.getSeries time-series)]
							     (let [index  (let [ind (.getIndex s (Millisecond. date))]
									    (if (< ind 0)
									      (dec (Math/abs ind))
									      ind))
								   data  (.getDataItem s index)
								   period (.getPeriod data)]
							       ((:set-value tbl-model) (.getKey s) (.getValue data))
							    

							     )))))))

							
		

    
    (.setDateFormatOverride (.getDomainAxis (.getPlot chart)) (SimpleDateFormat. "yy-MM-dd HH:mm:ss"))
   ;(let [right-axis (NumberAxis.)
;	  plot (.getPlot chart)]
 ;     (.setRangeAxis plot 1 right-axis)
  ;    (.setDataset plot 1 right-series)
   ;   )


    (doto (.getPlot chart)
      (.setDomainCrosshairVisible true)
      (.setBackgroundPaint Color/white)
      (.setRangeGridlinePaint Color/gray)
      (.setOutlineVisible false))
    
    (doto panel
      
      (.setLayout (BorderLayout.))
      (.add status-panel BorderLayout/SOUTH)
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
							 ;(.setDefaultRenderer Boolean
							;		      (proxy [TableCellRenderer] []
							;			(getTableCellRendererComponent [table value selected focus row column]
							;						       (doto (JCheckBox.)
					;							 (.setSelected (boolean value))))))
							 
						       (.setModel (:model tbl-model))
						       (.setCellSelectionEnabled false)
						       (.setName "table")
						      
						       (.addMouseListener mouse-table-adapter)
						       (.addMouseMotionListener mouse-table-adapter)
						       (.setAutoCreateRowSorter true)))))
	      (.setTopComponent (let [chart-panel (ChartPanel. chart)]
				       
				  ;(.setEntityCollection (.getChartRenderingInfo chart-panel) nil)     ;This may destroy some functionality, and does probably not improve too much
				  (doto chart-panel
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
									     )))))))) BorderLayout/CENTER))
    (let [show-column (.getColumn (.getColumnModel table) 1)]
      (.setCellEditor show-column (DefaultCellEditor. (JCheckBox.))))
    
 ;(ChartUtilities/applyCurrentTheme chart)				  
    {:panel panel
     :table-model tbl-model
     :connection-label connection-label
     :status-label status-label
     :add-to-table (:add-row tbl-model)
     :add-column (:add-column tbl-model)
     :chart chart
     :colors (color-cycle)
     :time-series time-series 
    :from (atom (Date. (- (System/currentTimeMillis) (* 1000 60 60))))
     :to (atom (Date.))
     :name "Monitor - Analysis Window"
     :transfer-queue (atom [])
     :num-to-render (atom 0)
     :num-rendered (atom 0)
     }))

