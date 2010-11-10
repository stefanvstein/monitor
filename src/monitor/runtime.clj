(ns monitor.runtime
  (:use (monitor commongui))
  (:use (clojure set))
  (:import (javax.swing UIManager JFrame JButton JOptionPane JMenuBar JMenu JMenuItem 
			JPanel JScrollPane JSplitPane JTable JLabel Box JDialog JComboBox
			JTextField WindowConstants JSpinner SpinnerDateModel SwingUtilities
			DefaultComboBoxModel GroupLayout GroupLayout$Alignment JPopupMenu
			BorderFactory))
  (:import (javax.swing.table TableCellRenderer AbstractTableModel))
  (:import (java.awt Dimension BorderLayout Color FlowLayout Component Point GridLayout 
		     GridBagLayout GridBagConstraints Insets))
  (:import (java.awt.event WindowAdapter ActionListener MouseAdapter))
  (:import (info.monitorenter.gui.chart Chart2D IAxisLabelFormatter TracePoint2D))
  (:import (info.monitorenter.gui.chart.labelformatters LabelFormatterDate LabelFormatterNumber))
  (:import (info.monitorenter.gui.chart.traces Trace2DLtdReplacing))
  (:import (info.monitorenter.gui.chart.rangepolicies RangePolicyHighestValues))
  (:import (info.monitorenter.gui.chart.axis AxisLinear))
  (:import (info.monitorenter.gui.chart.traces.painters TracePainterLine TracePainterDisc))
)
(def types ["Raw" "Average" "Mean" "Change/Second" "Change/Minute"])
(defn- with-type-added [type data]
  (if (map? data)
   (reduce (fn [r e]
	     (assoc r (assoc (key e) :type type) (val e)))
	   {} data)
   (reduce (fn [r e]
	     (conj r (assoc e :type type))) [] data)))

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
    
(defn data-with-doubles [data]
  (reduce (fn [r e]
	    (assoc r (key e) (reduce (fn [r d]
				       (assoc r (key d) (double (val d))))
				     (sorted-map)
				     (val e))))
	  {}
	  data))

(def runtimes (atom #{}))

(defn new-runtime-panel [frame]
  (let [panel (JPanel.)
	chart (Chart2D.)
	name-trace-row (atom {})
	watched-names (atom #{})
	table (JTable.)
	tbl-model (create-table-model
		   (fn [row-num]
;		     (println (str "Name-trace-row: " (class (key (first @name-trace-row)))))
;		     (println (str "watched names : " (class (first @watched-names))))
;		     (println (=  (key (first @name-trace-row)) (first @watched-names)))
		       
		     (swap! name-trace-row (fn [current]
					     (reduce (fn [r i]
						       (let [row (second (val i))]
							 (if (= row row-num)
							   (do
							     (.removeTrace chart (first (val i)))
							     (swap! watched-names (fn [c] (disj c (key i))))
							     (dissoc r (key i)))
							   (if (< row-num row)
							     (assoc r (key i) [(first (val i)) (dec (second (val i)))])
							     r)))) current current))))
		   (fn [row-num color]))
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

	axis-bottom (AxisLinear.)]
    (.addWindowListener frame (proxy [WindowAdapter] []
			       (windowClosed [e] ;(println "Window Closed")
					     ;(println (count @runtimes))
					     (swap! runtimes (fn [current]
							       (let [window (.getWindow e)]
								 (reduce (fn [r i]
									   (if (= window (SwingUtilities/windowForComponent (:panel i)))
									     (disj r i)
									     r)
									   ) current current))))
					     ;(println (count @runtimes))
					     )))
    (doto chart
      (.setMinimumSize (Dimension. 100 100))
      (.setPreferredSize (Dimension. 300 200))
      (.setAxisXBottom axis-bottom)
      (.setAxisYLeft (AxisLinear.))
      (.setPaintLabels false))

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
      (.setRangePolicy (RangePolicyHighestValues. (* 58 60 1000)))
      (.setPaintScale true)
      (.setPaintGrid true))  
    
      
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
	      (.setTopComponent chart ))))
(let [r
    {:panel panel 
     :chart chart
     :name-trace-row name-trace-row 
     :table-model (:model tbl-model)
     :add-to-table (:add-row tbl-model)
     :watched-names watched-names
     :add-column (:add-column tbl-model)
     :remove-row (:remove-row tbl-model)
     :colors (color-cycle)
     :name "Monitor - Runtime Window"
     }]
  (swap! runtimes #(conj % r)) 
  r)))

(def all-runtime-data (atom {}))
(def all-raw-names (atom #{}))
(def runtime-names-of-interest (atom #{}))



(def runtime-thread (atom nil))
	

(defn get-data-for [name]
  (let [data (get @all-runtime-data name)]
    ;(println (str name ":" data)) 
    (when (not (empty? data))
      (swap! runtime-names-of-interest (fn [current] (conj current name))))
    data))

(defn update-table-and-graphs [contents]
  (let [table-model (:table-model contents)
	current-columns (loop [i 0 r #{}]
			  (if (< i (.getColumnCount table-model))
			    (recur (inc i) (conj r (keyword (.getColumnName table-model i))))
			    r))
	name-trace-rows (:name-trace-row contents)
	chart (:chart contents)
	watched-names (deref (:watched-names contents))
	columns-in-watched (reduce (fn [r watched-name]
				     (reduce (fn [r a-name] (conj r (key a-name))) r watched-name))
				   #{:color} watched-names)
	columns-to-be-added (difference columns-in-watched current-columns)
	add-columns #(dorun (map (fn [c] ((:add-column contents) c)) %))
	new-trace-row #(let [color ((:colors contents))
			     trace  (doto (Trace2DLtdReplacing. 1000)
				    

				    
				      ) 
			     row (do ((:add-to-table contents) %1 color %2)
				     (- (.getRowCount table-model) 1))]
			 (.setColor trace color)
			 (.addTrace (:chart contents) trace)
			 [trace row])]
   (add-columns columns-to-be-added)
					;   (println (str (count (.getTraces (:chart contents))) "traces"))
   (dorun (map (fn [a-name] 
		 (let [unique-name a-name ;(str (merge (sorted-map) a-name))
		       trace-row (do
				   (if-let [trace-row (get @name-trace-rows unique-name)]
				     trace-row
				     (let [trace-row (new-trace-row a-name unique-name)]
				       (swap! name-trace-rows (fn [current] 
							       (assoc current unique-name trace-row)))
				       trace-row)))
		       trace (first trace-row)
		       ;data (with-nans (get-data-for a-name))]
		       data (get-data-for a-name)]
		   ;(println data)
		   (if (empty? data)
		     (do
		       (when-let [row  (second (get @name-trace-rows a-name))]
			 ((:remove-row contents) row)))
		     (do
		       (.removeAllPoints trace)
		       (doseq [d data]
			 (.addPoint trace (TracePoint2D. (.getTime (key d)) (val d))))))
		   )) 
	       watched-names))))

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
					  {}  @combomodels-on-center)]
			  (let [func  (.getSelectedItem func-combo)
				data (with-type-added func (data-with-doubles (get-data [name-values] func server)))]
			    (swap! (:watched-names contents) (fn [current] (apply conj current (keys data))))
			    (swap! all-raw-names (fn [current] (apply conj current (keys data))))
			    (swap! all-runtime-data (fn [all] (merge all data)))
			    (update-table-and-graphs contents)
			    )))]
    (.setBorder centerPanel (BorderFactory/createEmptyBorder 10 10 10 10))
    (doto dialog
      (.setDefaultCloseOperation WindowConstants/DISPOSE_ON_CLOSE)
      (.setResizable false))
    (.addActionListener close-button (proxy [ActionListener] [] (actionPerformed [event] (.dispose dialog))))
    (.addActionListener add-button (proxy [ActionListener] [] (actionPerformed [event] (onAdd contents))))
    (.addActionListener add-to-new-button (proxy [ActionListener] [] (actionPerformed [event]
										   (onAdd (@new-window-fn false)))))
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
		    names))
	(let [combo-action (create-comboaction @combomodels-on-center @combos-on-center [add-to-new-button add-button] names)]
	  (dorun (map (fn [combo] (.addActionListener combo combo-action)) @combos-on-center)))
				
	))
    dialog

))

(defn update-runtime-data []
  (swap! runtime-names-of-interest (fn [_] #{}))
  (dorun (map #(update-table-and-graphs %) @runtimes))
  (swap! all-raw-names (fn [current]
			     (reduce (fn [r i]
				       (conj r i)) 
				     #{}
				     @runtime-names-of-interest))))


  
(defn get-new-data [server]
  (let [
	data (reduce (fn [r type]
		       (merge r (with-type-added
				  type
				  (get-data (without-type (with-type type @all-raw-names)) type server))))
		     {} types)]
    (if (not (empty? data))

      (let [data-with-doubles (data-with-doubles data)]
	(swap! all-raw-names (fn [current] (apply conj current (keys data-with-doubles))))
	(swap! all-runtime-data (fn [all] (merge all data-with-doubles)))
	(SwingUtilities/invokeAndWait update-runtime-data))
      ;(println "There is no data")
      )))
