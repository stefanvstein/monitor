(ns monitor.analysis
  (:use [clojure stacktrace pprint]
        [monitor commongui tools mem clientdb]
        [clojure.contrib profile])

  (:import [javax.swing UIManager JFrame JButton JOptionPane JMenuBar JMenu JMenuItem 
            JPanel JScrollPane JSplitPane JTable JCheckBox JLabel Box JDialog JComboBox
            JTextField WindowConstants JSpinner SpinnerDateModel SwingUtilities
            DefaultComboBoxModel GroupLayout BoxLayout GroupLayout$Alignment JPopupMenu
            BorderFactory JSpinner$DateEditor  DefaultCellEditor Box KeyStroke JComponent
            JProgressBar]
           [javax.swing.table TableCellRenderer AbstractTableModel TableRowSorter]
           [java.awt Dimension BorderLayout Color FlowLayout Component Point GridLayout
            GridBagLayout GridBagConstraints Insets BasicStroke ]
           [java.awt.event WindowAdapter ActionListener MouseAdapter ComponentAdapter
            ItemEvent ItemListener KeyEvent]
           [java.net Socket UnknownHostException]
           [java.io ObjectInputStream IOException File]
           [java.util Calendar Date Comparator Properties]
           [java.text SimpleDateFormat DecimalFormat]
           [java.beans PropertyChangeListener]
           [org.jfree.chart.axis NumberAxis]
           [org.jfree.chart.plot ValueMarker]
           [org.jfree.chart.event ChartProgressEvent ChartProgressListener RendererChangeEvent]
           [org.jfree.chart ChartFactory ChartPanel JFreeChart ChartUtilities ChartMouseListener]
           [org.jfree.data.time TimeSeries TimeSeriesCollection TimeSeriesDataItem FixedMillisecond Millisecond]
           [org.jfree.chart.renderer.xy StandardXYItemRenderer XYSplineRenderer]
           [org.jfree.chart.entity AxisEntity]
           [monitor SplitDateSpinners]
           [com.google.common.io Files]
           [jdbm RecordManagerFactory RecordManagerOptions]))



(defn- date-order [d1 d2]
  (if (< (.getTime d1) (.getTime d2))
    [d1 d2]
    [d2 d1]))

(defn- update-chart [chart]

  (SwingUtilities/invokeLater (fn []
				(let [r (.getRenderer (.getPlot chart))
				      rc (RendererChangeEvent. r true)]
				  (.notifyListeners r rc)))))
  
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
												  (sorted-set)
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

  
(defn- last-from-timeserie "nil if none" [timeserie]
    (let [n (.getItemCount timeserie)
	  i (when-not (zero? n)
	      (.getDataItem timeserie (dec n)))]
    (when i
      (assoc {} (Date. (.getFirstMillisecond (.getPeriod i))) (.getValue i)))))
  

(defn- index-by-name [time-series-collection name]
  (let [n (.getSeriesCount time-series-collection)]
    (loop [i 0]
      (if (< i n)
	(if (= name (.getSeriesKey time-series-collection i))
	  i
      	  (recur (inc i)))
	nil))))
  


(defn on-add-to-analysis [from to shift name-values func-string
			  { time-series-coll :time-series
			   disable-col :disable-collection
                           progressbar :progress-bar
                           cancel-button :cancel-button
			   enable-col :enable-collection
			   colors :colors
			   ^JFreeChart chart  :chart
			   add-to-table :add-to-table
			   add-column :add-column
			   status-label :status-label
                           num-to-render :num-to-render
			   num-rendered :num-rendered
			   name-as-comparable :name-as-comparable
                           db :db-and-dir
                           db-entries :db-entries
                           cancellation :cancellation}
			  server]
  (let [my-cancellation @cancellation
        date-pairs-for-names (fn date-pairs-for-names [from to spec]
                               (let [get-names-according-to-spec-for (fn [from to]
                                                                       (according-to-specs (get-names from to server) [(apply hash-map spec)]))
                                     
                                     all-names-per-dates (reduce (fn [r date-pair]
                                                                   (assoc r date-pair (get-names-according-to-spec-for (first date-pair) (second date-pair))))
                                                                 {}
                                                                 (full-days-between from to))
                                     
                                     all-names-found (reduce #(if (seq %2)
                                                                (apply conj %1 %2)
                                                                %1)
                                                             #{}
                                                             (vals all-names-per-dates))]
                                 (reduce (fn [r name]
                                           (if (> @cancellation my-cancellation)
                                             {}
                                             (assoc r name (filter identity
                                                                   (map (fn [i]
                                                                          (when (contains? (val i) name)
                                                                            (key i)))
                                                                        all-names-per-dates)))))
                                         {}
                                         all-names-found)))
        stop-chart-notify (fn stop-chart-notify []
                            (.setNotify chart false)
                            (disable-col))
	start-chart-notify (fn start-chart-notify []
                             (.setNotify chart true)
			     (enable-col))

        
                         
	shift-time (if (= 0 shift)
		     identity
                     (let [shifted-string (let [df (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")]
                         (str (.format df from)
                              " to "
                              (.format df (Date. (long (+ (.getTime ^Date from) shift))))))]
                       (fn [a] (let [add-shifted-key (fn [a] (reduce (fn [r a-data]
                                                                       (assoc r (assoc (key a-data)
                                                                                  :shifted
                                                                                  shifted-string)
                                                                            (val a-data)))
                                                                   {} a))
                                   
                                   data-shifted (with-keys-shifted a identity (fn [#^Date d] (Date. (long (+ (.getTime d) shift)))))]
                               
                               (add-shifted-key data-shifted)))))

	transform-all (fn [data func]
                        (reduce (fn [r e]
                                  (assoc r (key e) (transform (val e) func)))
                                {}
                                data))
        create-new-time-serie (fn [data-key]
				(let [identifier (name-as-comparable data-key)
				      serie (TimeSeries. identifier)
				      color (colors)]
                                  (. time-series-coll addSeries serie)
                                  (let [visible-fn (fn ([]
                                                          (if-let [index (index-by-name time-series-coll identifier)]
                                                            (.. chart (getPlot) (getRenderer) (getItemVisible index 0 ))
                                                            false))
                                                     ([visible]
                                                        (when-let [index (index-by-name time-series-coll identifier)]
                                                          (.. chart (getPlot) (getRenderer) (setSeriesVisible index visible)))))
                                        new-index (dec (count (.getSeries time-series-coll)))]
                                        ;(println "new-index" new-index)
                                    (doto (.. chart 
                                              (getPlot) (getRenderer))
                                      (.setSeriesPaint new-index color)
                                      (.setSeriesStroke new-index (BasicStroke. 2.0)))
                                    (add-to-table data-key color visible-fn)
                                    (doseq [i (keys data-key)]
                                      (add-column i))
                                    serie)))
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

        store-to-disk (fn [keys data]
                        ;(println "store " keys (class keys))
                        (try (let [old (get @db-entries keys)]
                               (let [id (.insert (:db db) data)]
                                 (dosync (alter db-entries assoc keys id)))
                               (if old
                                 (doto (:db db)
                                   (.delete old)
                                   (.commit)
                                   (.defrag)
                                   (.commit))
                                 (.commit (:db db))))
                             (catch IOException _)))
        get-from-disk (fn [keys]
                                        ;                       (println "get" keys (class keys))
                        (try
                          (if-let [id (get @db-entries keys)]
                            (.fetch (:db db) id)
                            (sorted-map))
                          (catch IOException _)))
	
        get-data-for (fn get-data-for [name date-pairs]
                       (let [update (fn [result data]
                                                (if (seq (val data))
                                                  (update-in result [(key data)] merge (val data))
                                                  result))]
                         (reduce (fn [r date-pair]
                                   (reduce update r (shift-time (get-data (first date-pair)
                                                                          (second date-pair)
                                                                          (reduce #(conj %1 (key %2) (val %2)) [] name)
                                                                          server))))
                                 {}
                                 date-pairs)))
        
        update-method (fn update-method [to-update data to-render render-count]
                        (let [update (fn update []
                                       (.setVisible progressbar true)
                                       (.setVisible cancel-button true) 
                                       (.setMaximum progressbar @num-to-render)
                                       (.setString progressbar (str @num-rendered "/" @num-to-render))
                                       
                                       (stop-chart-notify)
                                       (try
                                       (doseq [data data]

                                         (let [data-key (assoc (key data) :type func-string)
                                               all-values (merge (get-from-disk (name-as-comparable data-key)) (val data))]
                                           (store-to-disk  (name-as-comparable data-key) all-values)
                                           (let [calc-data (reduce-samples (with-nans (transform all-values func-string) nan-distance))]
                                             (let [^TimeSeries time-serie (if-let [serie (. time-series-coll getSeries (name-as-comparable data-key))]
                                                                serie
                                                                (create-new-time-serie data-key))]
                                               (.clear time-serie)
                                               (doseq [d calc-data]
                                                 (.add time-serie (TimeSeriesDataItem.
                                                                   (Millisecond. (key d))
                                                                   ^Number (val d))))))))
                                       
                                       (finally (start-chart-notify)
                                                (.setValue progressbar (swap! num-rendered inc))
                                                (.setString progressbar (str @num-rendered "/" @num-to-render)))))]
                          (if-let [d (let [d (first to-update)]
                                       (if (> @cancellation my-cancellation)
                                         (do
                                           (swap! num-rendered - render-count)
                                           (swap! num-to-render - to-render)
                                           nil)
                                         d))]
                              (do (future
                                    (let [data (get-data-for (key d) (val d))]
                                      (SwingUtilities/invokeLater (fn [] (update-method (next to-update) data to-render (inc render-count))))))
                                  (update))
                              (do (update)
                                  (update-chart chart)
                                  (when (>= @num-rendered @num-to-render)
                                    (.setVisible cancel-button false)
                                    (doto progressbar
                                      (.setVisible false)
                                      (.setMaximum 0)
                                      (.setValue 0)
                                      (.setString ""))
                                    (reset! num-to-render 0)
                                    (reset! num-rendered 0))))))]

          (future
            (SwingUtilities/invokeLater (fn [] (when-not (.isVisible progressbar)
                                                 (.setVisible cancel-button true)
                                                 (.setVisible progressbar true)
                                                 (.setString progressbar "Getting names"))))
            (let [to-update (date-pairs-for-names from to name-values)]
              (when (seq to-update)
                (SwingUtilities/invokeLater (fn []

                                              (swap! num-to-render + (count to-update))
                                              (.setMaximum progressbar @num-to-render)
                                              (.setString progressbar (str @num-rendered "/" @num-to-render))
                                              ))
                (let [data (get-data-for (key (first to-update)) (val (first to-update)))]
                  (SwingUtilities/invokeLater (fn []
                                                (update-method (next to-update) data (count to-update) 0)))))))))



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
	
	(let [close-listener (proxy [ActionListener] [] (actionPerformed [event] (.dispose dialog)))
	      add-listener (proxy [ActionListener] [] (actionPerformed [event] (when (.isEnabled add)
										 (onAdd contents))))
	      add-new-listener (proxy [ActionListener] [] (actionPerformed [event] (when (.isEnabled add-new-window)
										     (onAdd (@new-window-fn true)))))
	      ]
	  (doto dialog
	    (.setDefaultCloseOperation WindowConstants/DISPOSE_ON_CLOSE)
	    (.setResizable false))
	  (.registerKeyboardAction (.getRootPane dialog) add-listener (KeyStroke/getKeyStroke KeyEvent/VK_ENTER java.awt.event.InputEvent/ALT_DOWN_MASK) JComponent/WHEN_IN_FOCUSED_WINDOW)
	  (.registerKeyboardAction (.getRootPane dialog) add-new-listener (KeyStroke/getKeyStroke KeyEvent/VK_ENTER java.awt.event.InputEvent/SHIFT_DOWN_MASK) JComponent/WHEN_IN_FOCUSED_WINDOW)
	  (.registerKeyboardAction (.getRootPane dialog) close-listener (KeyStroke/getKeyStroke KeyEvent/VK_ESCAPE 0) JComponent/WHEN_IN_FOCUSED_WINDOW)
	  (.addActionListener close close-listener)
	  (.addActionListener add add-listener)
	  (.addActionListener add-new-window add-new-listener ))
	
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
						  (let [names (grouped-maps (get-names from to server))]
						   
						    (swap! all-names (fn [_]
								       (reduce (fn [r e]
                                                                                 (reduce (fn [r e]
											   (conj r e)) r e))
                                                                               #{} (vals names)) 
								       ))
						    
						    (let [panel-and-models (on-update names [add add-new-window])
							  panel (first panel-and-models)
							  content-pane (.getContentPane dialog)]
						      (.remove content-pane @center-panel)
						      (reset! center-panel panel)
						      (reset! combomodels-on-center (second panel-and-models))
						      (.add content-pane panel BorderLayout/CENTER)
						      )
						    (.pack dialog))))
				     update-listener (proxy [ActionListener] [] (actionPerformed [_] (onUpdate) ))]
				 (. update addActionListener update-listener)
				 (.registerKeyboardAction (.getRootPane dialog) update-listener (KeyStroke/getKeyStroke KeyEvent/VK_U java.awt.event.InputEvent/CTRL_DOWN_MASK) JComponent/WHEN_IN_FOCUSED_WINDOW)
				 update)))
	  
	dialog)))



(defn new-analysis-panel []

  (let [db-and-dir (remove-on-terminate (create-db))
        db-entries (ref {})
        status-label (JLabel. " ")
        progressbar (doto (JProgressBar.)
                      (.setVisible false)
                      (.setStringPainted true))
        cancellation (atom 0)
        cancel-button (doto (JButton. "Cancel")
                        (.setVisible false)
                        (.addActionListener (proxy [ActionListener] []
                                              (actionPerformed [_]
                                                (swap! cancellation inc)))))
	connection-label (JLabel. " ")
	comparables-and-names (atom {})			   	
	name-as-comparable (memoize (fn [name]
				    (let [c (str name)]
				      (swap! comparables-and-names assoc c name)
				      c)))
	comparable-as-name (fn [c] (get @comparables-and-names c))
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
										     (.invalidateLayout layout p)))))))
                         (.add (Box/createRigidArea (Dimension. 5 0)))
                         (.add cancel-button)
                         (.add (Box/createRigidArea (Dimension. 2 0)))
                         (.add progressbar)))

	panel (JPanel.)
	enabled-collection-notify (atom true)
		
	time-series (proxy [TimeSeriesCollection] []
		      (fireDatasetChanged []
					  (when @enabled-collection-notify
					    (proxy-super fireDatasetChanged))))
		     
	disable-timeseriescollection (fn []
				       (reset! enabled-collection-notify false))
	
	enable-timeseriescollection (fn []
				      (reset! enabled-collection-notify true)
				      (.fireDatasetChanged time-series))

;	right-series (TimeSeriesCollection.)
	chart (doto (ChartFactory/createTimeSeriesChart 
					      nil nil nil 
					      time-series false false false)
		
		)
		
	remove-from-disk (fn [name]
                           (let [to-remove (dosync (let [to-remove (get @db-entries name)]
                                                     (alter db-entries dissoc name)
                                                     to-remove))]
                             (future (try
                                       (doto (:db db-and-dir)
                                            (.remove to-remove)
                                            (.commit)
                                            (.defrag)
                                            (.commit))
                                          (catch IOException e
                                            (println e))))))
                           

	tbl-model (create-table-model
		   (fn [row] 
		     (.removeSeries time-series (.getSeries time-series (name-as-comparable row)))
                     (remove-from-disk (name-as-comparable row)))
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
        crosshair-domain-marker (atom nil)
        mouseToX (fn mouseToX [event chart-panel]
          (let [entity (.getEntity event)]
            (when (instance? AxisEntity entity)
              (let [axis (.getAxis entity)
                    plot (.getPlot axis)
                    domain-axis (.getDomainAxis plot)]
                (when (= domain-axis axis)
                  (let [p (.translateScreenToJava2D chart-panel (.getPoint (.getTrigger event)))
                        plotArea (.getScreenDataArea chart-panel)
                        plot (.getPlot (.getChart event))
                        axis (.getDomainAxis plot)]
                    (.java2DToValue axis (.getX p) plotArea (.getDomainAxisEdge plot))))))))
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
                                                
						row (do ;(println "convertRowIndexToModel")
                                                        (try
                                                          (.convertRowIndexToModel table (.rowAtPoint table (Point. x y)))
                                                          (finally #_(println "Done convertRowIndexToModel"))))
						renderer (.. chart (getPlot) (getRenderer))
						stroke (.getSeriesStroke renderer row)]
					    (when-let [hl @highlighted]
					      (when-not (= (first hl) row)
						(.setSeriesStroke renderer (first hl) (second hl))
						(reset! highlighted nil)
						))
					    (when-not @highlighted
					      (.setSeriesStroke renderer row (BasicStroke. 4.0)) 
					      (reset! highlighted [row stroke]))))
			      
			      (mouseExited [event]
					   (when-let [hl @highlighted]
					     (let [renderer (.. chart (getPlot) (getRenderer))]
					       (.setSeriesStroke renderer (first hl) (second hl))
											       (reset! highlighted nil)) 
					     )))]
					(.setForegroundAlpha (.getPlot chart) 0.8)
					(.setDrawSeriesLineAsPath (.getRenderer (.getPlot chart)) true)
					;(.setRangeCrosshairLockedOnData (.getPlot chart) false)
    ;(.setRenderer (.getPlot chart) (XYSplineRenderer. 1))

		(.addProgressListener chart (proxy [ChartProgressListener] []
					      (chartProgress [e]
                                                (when (= (.getType e) ChartProgressEvent/DRAWING_FINISHED)
                                                  (when-let [crosshair-domain-marker @crosshair-domain-marker]
                                                    (let [date (let [l (long (.getValue crosshair-domain-marker))]
                                                                (if (zero? l)
                                                                      nil
                                                                      (Date. l)))]
                                                      (.setText status-label (.format (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss") date))
                                                    (doseq [s (.getSeries time-series)]
                                                      (let [index  (let [ind (.getIndex s (Millisecond. date))]
                                                                     (if (< ind 0)
                                                                       (dec (Math/abs ind))
                                                                       ind))
                                                            data  (if (= index (.getItemCount s))
                                                                    nil
                                                                    (.getDataItem s index))
                                                            
                                                            period (if data
                                                                     (.getPeriod data)
                                                                     nil)]
                                                        ((:set-value tbl-model)
                                                         (comparable-as-name (.getKey s))
                                                         (if data
                                                           (let [v (.getValue data)]
                                                             (if (isNan v)
                                                               "" v))
                                                           ""))))))))))

							
		

    
    (.setDateFormatOverride (.getDomainAxis (.getPlot chart)) (SimpleDateFormat. "yy-MM-dd HH:mm:ss"))

                                        



    (doto (.getPlot chart)
      (.setBackgroundPaint Color/white)
      (.setRangeGridlinesVisible true)
      (.setRangeGridlinePaint Color/gray)
      (.setDomainGridlinePaint Color/gray)
      (.setDomainGridlinesVisible true)
      (.setDomainMinorGridlinesVisible true)
      (.setOutlineVisible false)
      (.setDomainPannable true)
      (.setRangePannable true))
    
    
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
							 (.setDefaultRenderer Number (proxy [TableCellRenderer] []
							    (getTableCellRendererComponent 
							   [table value selected focus row column]
							   (doto (JLabel.)
							     (.setHorizontalAlignment JLabel/RIGHT)
							     (.setText (if (instance? Number value)
									 (let [d (DecimalFormat. "#,###.###")]
									   (.setDecimalFormatSymbols d (doto (.getDecimalFormatSymbols d)
													 (.setDecimalSeparator  \.)
													 (.setGroupingSeparator \ )))
									   (. d format value))
									 value))))))
									 
						       (.setModel (:model tbl-model))
						       (.setCellSelectionEnabled false)
						       (.setName "table")
						      
						       (.addMouseListener mouse-table-adapter)
						       (.addMouseMotionListener mouse-table-adapter)
					;						       (.setAutoCreateRowSorter false)
						      ;modelStructureChanged
						       (.setRowSorter (create-row-sorter-with-Number-at (:model tbl-model) 2))
							))))
	      (.setTopComponent (let [chart-panel (ChartPanel. chart)]
				  
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
                                                                 ))))
                                    (.addChartMouseListener (proxy [ChartMouseListener] []
                                                              (chartMouseClicked [event]
                                                                (when-let [v (mouseToX event chart-panel)]
                                                                  (let [plot (.getPlot chart)] 
                                                                  (when @crosshair-domain-marker

                                                                      (.removeDomainMarker plot @crosshair-domain-marker))
                                                                  (.addDomainMarker plot (swap! crosshair-domain-marker (fn [_] (doto (ValueMarker. v)
                                                                                                                                  (.setPaint Color/gray))))))))
                                                              (chartMouseMoved [events]))))))) BorderLayout/CENTER))
;     (let [d (.getComparator (.getRowSorter table) 2)]
;					      (println d)
;					      (println (.compare d 3 2)))
    (let [show-column (.getColumn (.getColumnModel table) 1)]
      (.setCellEditor show-column (DefaultCellEditor. (JCheckBox.))))
    
					;(ChartUtilities/applyCurrentTheme chart)
   
    {:progress-bar progressbar
     :cancel-button cancel-button
     :db-and-dir db-and-dir
     :db-entries db-entries
     :crosshair-domain-marker crosshair-domain-marker
     :disable-collection disable-timeseriescollection
     :enable-collection enable-timeseriescollection
     :name-as-comparable name-as-comparable
     :comparable-as-name comparable-as-name 
     :data (atom {})
     :panel panel
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
     :num-to-render (atom 0)
     :num-rendered (atom 0)
     :cancellation cancellation
     }))

