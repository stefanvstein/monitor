(ns monitor.runtime
  (:use (monitor commongui))
  (:use (clojure set pprint))
  (:import (java.util Comparator Date))
  (:import (javax.swing UIManager JFrame JButton JOptionPane JMenuBar JMenu JMenuItem 
			JPanel JScrollPane JSplitPane JTable JLabel Box JDialog JComboBox
			JTextField WindowConstants JSpinner SpinnerDateModel SwingUtilities
			DefaultComboBoxModel GroupLayout GroupLayout$Alignment JPopupMenu
			BorderFactory KeyStroke JComponent))
  (:import (javax.swing.table TableCellRenderer AbstractTableModel TableRowSorter))
  (:import (java.awt Dimension BorderLayout Color FlowLayout Component Point GridLayout 
		     GridBagLayout GridBagConstraints Insets BasicStroke))
  (:import (java.awt.event WindowAdapter ActionListener MouseAdapter KeyEvent))
  (:import (java.text DecimalFormat))
  (:import (info.monitorenter.gui.chart Chart2D IAxisLabelFormatter TracePoint2D))
  (:import (info.monitorenter.gui.chart.labelformatters LabelFormatterDate LabelFormatterNumber))
  (:import (info.monitorenter.gui.chart.traces Trace2DLtdReplacing Trace2DSimple))
  (:import (info.monitorenter.gui.chart.rangepolicies RangePolicyHighestValues RangePolicyFixedViewport))
  (:import (info.monitorenter.gui.chart.axis AxisLinear))
  (:import (info.monitorenter.gui.chart.traces.painters TracePainterLine TracePainterDisc))
  (:import (info.monitorenter.util Range))
  )

(declare get-new-data)

(defn- according-to-specs [names specs]
  (into #{}
	(filter identity
		(for [name names spec specs]
		  (when (= spec (select-keys name (keys spec)))
		    name)))))

(def types ["Raw" "Average" "Mean" "Change/Second" "Change/Minute"])

(defn- with-type-added [type data]
  (when (seq data)
    (if (map? data)
      
      (if (map? (key (first data)))
	(reduce (fn [r e]
		  (assoc r (assoc (key e) :type type) (val e)))
		{} data)

	(assoc data :type type))
      (reduce (fn [r e]
		(conj r (assoc e :type type))) [] data))))

(defn with-type [type data]
  (if (map? data)
    (into {} (filter (fn [e]
		       (= type (:type (key e))))
		     data))
    (filter (fn [e] (= type (:type e))) data)))
    

(defn- without-type [data]
  (if (map? data)
    (reduce (fn [r e]
	      (assoc r (dissoc (key e) :type) (val e)))
	    {} data)
   
	(reduce (fn [r e]
		  (conj r (dissoc e :type))) [] data))) 

(defn simple-data-with-doubles [data]
   (reduce (fn [r d]
	     (assoc r (key d) (double (val d))))
	   (sorted-map)
	   data))


(def runtimes (atom #{}))

(defn new-runtime-panel [frame]
  (let [panel (JPanel.)
	connection-label (JLabel. " ")
	chart (doto (Chart2D.)
		#_(.setUseAntialiasing true))
	name-trace (ref {})
	watched-specs (atom #{})
	removed-names (atom #{})
	table (JTable.)
	tbl-model (create-table-model
		   (fn remove-graph [name]
		     (let [trace (get @name-trace name)]
			(dosync
			 (alter name-trace dissoc name))
			(.removeTrace chart trace)
			(swap! removed-names conj name)
			))
		   (fn recolor [row-num color]))
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
	panel (JPanel.)

	axis-bottom (AxisLinear.)
		      
		      ]
    (.addWindowListener frame (proxy [WindowAdapter] []
			       (windowClosed [e] 
					     (swap! runtimes (fn [current]
							       (let [window (.getWindow e)]
								 (reduce (fn [r i]
									   (if (= window (SwingUtilities/windowForComponent (:panel i)))
									     (disj r i)
									     r)
									   ) current current)))))))
    (doto chart
      (.setMinimumSize (Dimension. 100 100))
      (.setPreferredSize (Dimension. 400 300))
      (.setAxisXBottom axis-bottom)
      (.setAxisYLeft (AxisLinear.))
      (.setPaintLabels false))

    (.setRangePolicy axis-bottom (RangePolicyFixedViewport. (Range. (- (System/currentTimeMillis) (* 1000 60 55)) (System/currentTimeMillis))))
    (let [nf (let [df (java.text.DecimalFormat.)
		   syms (.getDecimalFormatSymbols df)]
	       (.setGroupingSeparator syms \space)
	       (.setDecimalFormatSymbols df syms)	       
	       df)]
      (doto (first (.getAxesYLeft chart))
	(.setFormatter (proxy [LabelFormatterNumber] [nf]))
	(.setPaintScale true)
	(.setPaintGrid true)))

    (doto (first (.getAxesXBottom chart))
      (.setFormatter (LabelFormatterDate. (java.text.SimpleDateFormat. "HH:mm:ss")))
					;      (.setRangePolicy (RangePolicyHighestValues. (* 58 60 1000)))
      
      (.setPaintScale true)
      (.setPaintGrid true))  

    
      
    (doto panel
      (.setLayout (BorderLayout.))
      (.add connection-label BorderLayout/SOUTH)
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
						       (.addMouseListener (proxy [MouseAdapter] []
									    (mousePressed [event]
											  (if (.isPopupTrigger event)
											    (popup event)))
									    (mouseReleased [event]
											   (when (.isPopupTrigger event)
											     (popup event)))
									     (mouseExited [event]
											  (doseq [trace (vals @name-trace)]
											    (when-not (= 2.0 (.getLineWidth (.getStroke trace)))
											      (.setStroke trace (BasicStroke. 2.0)))))))

						       (.addMouseMotionListener  (proxy [MouseAdapter] []
									    (mouseMoved [event]
											(let [row (.convertRowIndexToModel table (.rowAtPoint table
																	      (Point. (.getX event)
																		      (.getY event))))
											      row-data (get @(:rows tbl-model) row)
											      the-trace (get @name-trace (:data row-data))]
											  (doseq [trace (vals @name-trace)]
											    (if (= trace the-trace)
											      (when-not (= 3.0 (.getLineWidth (.getStroke trace)))
												(.setStroke trace (BasicStroke. 3.0)))
											      (when-not (= 2.0 (.getLineWidth (.getStroke trace)))
												(.setStroke trace (BasicStroke. 2.0)))))))))
				;		       (.setAutoCreateRowSorter true)
						       (.setRowSorter (create-row-sorter-with-Number-at (:model tbl-model) 2))
						       ))))
	      (.setTopComponent chart ))))
(let [r
    {:panel panel 
     :chart chart
     :connection-label connection-label
     :name-trace name-trace
     :removed-names removed-names
     :table-model tbl-model
     :add-to-table (:add-row tbl-model)
     :watched-specs watched-specs
     :add-column (:add-column tbl-model)
     :remove-row (:remove-row tbl-model)
     :colors (color-cycle)
     :name "Monitor - Runtime Window"
     }]
  (swap! runtimes #(conj % r)) 
  r)))

(def all-runtime-data (atom {}))
;(def all-raw-names (atom #{}))
;(def runtime-names-of-interest (atom #{}))

(defn get-data-for [name]
  (get @all-runtime-data name)
  #_(let [data (get @all-runtime-data name)]
    (when (not (empty? data))
      (swap! runtime-names-of-interest (fn [current] (conj current name))))
    data))


  (defn xrange
    [date chart]
       (let [range (Range. (- (.getTime date) (* 1000 60 55)) (+ (.getTime date) (* 1000 60 1)))]
	 (.setRange (.getAxisX chart) range)
	 range))

(defn update-table-and-graphs [contents]
  (let [tbl-modl (:table-model contents)
	table-model (:model tbl-modl)
	current-columns (loop [i 0 r #{}]
			  (if (< i (.getColumnCount table-model))
			    (recur (inc i) (conj r (keyword (.getColumnName table-model i))))
			    r))
	name-trace (:name-trace contents)
	chart (:chart contents)
					;watched-names (deref (:watched-names contents))
	watched-names (let [names (according-to-specs (keys @all-runtime-data) @(:watched-specs contents))
			    dif (difference names @(:removed-names contents))]
			dif)
	columns-in-watched (reduce (fn [r watched-name]
				     (reduce (fn [r a-name] (conj r (key a-name))) r watched-name))
				   #{:color} watched-names)
	columns-to-be-added (difference columns-in-watched current-columns)

	add-columns #(doseq [c %]
			    ((:add-column contents) c))

	new-trace #(let [color ((:colors contents))
			 trace  (doto (Trace2DSimple.)
				  (.setStroke (BasicStroke. 2.0)))
			 visible-fn (fn ([] (.isVisible trace))
				      ([visible] (.setVisible trace visible)))
			 row (do ((:add-to-table contents) %1 color visible-fn)
				 (- (.getRowCount table-model) 1))]
			 (.setColor trace color)
			 (.addTrace (:chart contents) trace)
			 trace)]

    (add-columns columns-to-be-added)


    (let [range (xrange (Date.) chart)
	  updated-names (reduce (fn [r a-name] 
				   (let [trace (dosync
						(if-let [trace (get @name-trace a-name)]
						  trace
						  (let [trace (new-trace a-name)]
						    (alter name-trace assoc a-name trace)
						    trace)))
					 
					
					 data (get-data-for a-name)]
				     (.removeAllPoints trace)	
				     (when (seq data)
				       (doseq [d  (simple-data-with-doubles data)]
					 (when (<= (.getMin range) (.getTime (key d)))
					   (.addPoint trace (TracePoint2D. (.getTime (key d)) (val d)))))
				       ((:set-value tbl-modl) a-name (val (last data)))
				       )
				   (conj r a-name )))
				#{} watched-names)]

					;each empty trace, ((:remove-row contents) name)
      (doseq [e @name-trace]
	(if (= 0 (.getSize (val e)))
	  ((:remove-row contents) (key e))
	  (let [t30s (* 1000 100)
		diff (- (.getMax range) (.getMaxX (val e)))]

	    (if (> diff  t30s )
	      ((:set-value (:table-model contents)) (key e) "")))) )
		    
				
      #_(doseq [to-remove (difference (set (keys @name-trace)) (into #{} (for [n  updated-names]
									(first n))))]
	((:remove-row contents) to-remove))

      )))
	      

(defn runtime-add-dialog [contents server]
  (let [dialog (JDialog. (SwingUtilities/windowForComponent (:panel contents)) "Add" false)
	add-button (let [add (JButton. "Add")]
		     (.setEnabled add false)
		     add)
	add-to-new-button (let [add (JButton. "Add New")]
		     (.setEnabled add false)
		     add)
	func-combo (JComboBox. (to-array types))
	close-button (JButton. "Close")
	centerPanel (JPanel.)
	combomodels-on-center (atom {})
	onAdd (fn [contents] 
			(let [name-values (reduce (fn [result name-combomodel] 
					    (let [the-value (.getSelectedItem (val name-combomodel))]
					      (if (not (= "" the-value))
						(assoc result (key name-combomodel) the-value)
						result)))
						  {}  @combomodels-on-center)
			      name-values-with-type (with-type-added (.getSelectedItem func-combo) name-values)]
			  (swap! (:removed-names contents) (fn [removed] (apply disj removed (filter #(when (= name-values-with-type (select-keys % (keys name-values-with-type)))
													%)
												     removed)))) 
			  (swap! (:watched-specs contents) conj name-values-with-type)
			  (get-new-data server)
			  ))]
    (.setBorder centerPanel (BorderFactory/createEmptyBorder 10 10 10 10))
    (let [close-listener (proxy [ActionListener] [] (actionPerformed [event] (.dispose dialog)))
	  add-listener (proxy [ActionListener] [] (actionPerformed [event] (when (.isEnabled add-button)
									     (onAdd contents))))
	  add-new-listener (proxy [ActionListener] [] (actionPerformed [event] (when (.isEnabled add-to-new-button)
									     (onAdd (@new-window-fn false)))))]
      (doto dialog
	(.setDefaultCloseOperation WindowConstants/DISPOSE_ON_CLOSE)
	(.setResizable false))
      (.registerKeyboardAction (.getRootPane dialog)
			       add-listener (KeyStroke/getKeyStroke
					     KeyEvent/VK_ENTER
					     java.awt.event.InputEvent/ALT_DOWN_MASK)
			       JComponent/WHEN_IN_FOCUSED_WINDOW)
      (.registerKeyboardAction (.getRootPane dialog)
			       add-new-listener (KeyStroke/getKeyStroke
						 KeyEvent/VK_ENTER
						 java.awt.event.InputEvent/SHIFT_DOWN_MASK)
			       JComponent/WHEN_IN_FOCUSED_WINDOW)
      (.registerKeyboardAction (.getRootPane dialog)
			       close-listener (KeyStroke/getKeyStroke
					       KeyEvent/VK_ESCAPE
					       0)
			       JComponent/WHEN_IN_FOCUSED_WINDOW)
      (.addActionListener close-button close-listener)
      (.addActionListener add-button add-listener)
      (.addActionListener add-to-new-button add-new-listener))
    (let [contentPane (.getContentPane dialog)]
      (. contentPane add centerPanel BorderLayout/CENTER)
      (. contentPane add (doto (JPanel.)
			   (.setLayout (FlowLayout. FlowLayout/CENTER))
			   (.add func-combo)
			   (.add add-button)
			   (.add add-to-new-button)
			   (.add close-button)) 
	 BorderLayout/SOUTH))
    (let [names (get-names (server))]
      (.setLayout centerPanel (GridBagLayout.))
      (let [labelsOnCenter (atom [])
	    combos-on-center (atom [])]
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
					    (sorted-set) (val a-name))]
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
		    names))
	(let [combo-action (create-comboaction @combomodels-on-center @combos-on-center [add-to-new-button add-button] names)]
	  (dorun (map (fn [combo] (.addActionListener combo combo-action)) @combos-on-center)))
				
	))
    dialog))

(defn update-runtime-data []
  (doseq [rt  @runtimes]
    (update-table-and-graphs rt)))

  
  (defn get-new-data [server]
  ;(setXRange)
  (try
    (let [raw-type (first types)
	  transformations-according-to-specs (fn [data specs]
					      (into {}
						    (filter identity
							    (for [d data spec specs]
							      (let [the-key (key d)
								    spec-without (dissoc spec :type)]
								(when (= spec-without (select-keys the-key (keys spec-without)))
								  [(assoc the-key :type (:type spec)) (transform (val d) (:type spec))]
								  ))))))
	  all-specs (reduce (fn [r e]
			      (when-let [watched @(:watched-specs e)]
				(when (seq watched)
				  (apply conj r watched))))
			  #{}
			  @runtimes)
	  non-raw-specs (filter #(when-not (= (:type %) raw-type)
				   %) all-specs)
	
	  names-of-interest-now  (according-to-specs (reduce (fn [result a-map]
							       (conj result (names-as-keyworded a-map))) [] (.rawLiveNames (server)))
						     (without-type all-specs))
	  data (with-type-added raw-type (get-data names-of-interest-now server))]
      (when (seq data)
	(let [data (merge data (transformations-according-to-specs data non-raw-specs))]
	 (reset! all-runtime-data data)))
	  
	    (if (SwingUtilities/isEventDispatchThread)
	      (update-runtime-data)
	      (SwingUtilities/invokeAndWait update-runtime-data)))
  (catch Exception e
    (.printStackTrace e))))
