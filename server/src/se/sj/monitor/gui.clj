(ns se.sj.monitor.gui
  (:use (clojure stacktrace))
  (:import (javax.swing UIManager JFrame JButton JOptionPane JMenuBar JMenu JMenuItem 
			JPanel JScrollPane JSplitPane JTable JLabel Box JDialog JComboBox
			JTextField WindowConstants JSpinner SpinnerDateModel SwingUtilities
			DefaultComboBoxModel GroupLayout GroupLayout$Alignment))

  (:import (javax.swing.table TableCellRenderer AbstractTableModel))
  (:import (java.awt Dimension BorderLayout Color FlowLayout Component))
  (:import (java.awt.event WindowAdapter ActionListener))
  (:import (java.net Socket UnknownHostException))
  (:import (java.io ObjectInputStream))
  (:import (org.jfree.chart ChartFactory ChartPanel JFreeChart))
  (:import (org.jfree.data.time TimeSeries TimeSeriesCollection TimeSeriesDataItem Millisecond)))

(def frames (atom #{}))

(defn redraw []
     (dorun (map (fn [frame] (.revalidate frame)) @frames)))

(def *shutdown* #(println "Shutdown :)"))
(def current-server (atom nil))
(try
 (UIManager/setLookAndFeel (UIManager/getSystemLookAndFeelClassName))
(catch Exception _))

(declare connect)

(defn connect-dialog [ parent]
  (let [dialog (JDialog. parent "Connection" false)
	ok (JButton. "Ok")
	cancel (JButton. "Cancel")
	host (JTextField.)
	port (JTextField.)
	onOk #(let [host (.getText host)
		       port (.getText port)]
		   (try
		    (let [port (Integer/parseInt port)]
		      (if (> 0 port)
			(JOptionPane/showMessageDialog dialog 
						       "Port can't be negative" 
						       "Error" JOptionPane/ERROR_MESSAGE) 
			(if (= 0 (. (.trim host) length))
			  (JOptionPane/showMessageDialog dialog 
							 "Host can't be empty" "Error" 
							 JOptionPane/ERROR_MESSAGE)
			  (do
			    (connect host port)
			    (.dispose dialog))
			  )))
		    (catch Exception e
		    (let [cause (root-cause e)]
		    (condp = (class cause)
		      NumberFormatException (JOptionPane/showMessageDialog 
					     dialog "Port need to be integer value" 
					     "Error" JOptionPane/ERROR_MESSAGE)
			UnknownHostException (JOptionPane/showMessageDialog 
					      dialog "Unknown host" 
					      "Error" JOptionPane/ERROR_MESSAGE)
			(JOptionPane/showMessageDialog 
			 dialog (.getMessage cause) "Error" JOptionPane/ERROR_MESSAGE))))))
	onCancel #(.dispose dialog)
	hostLabel (JLabel. "Host")
	portLabel (JLabel. "Port")]
    (doto dialog
      (.setDefaultCloseOperation WindowConstants/DISPOSE_ON_CLOSE)
      (.setResizable false))
    (doto ok 
      (.addActionListener (proxy [ActionListener] [] (actionPerformed [event]  (onOk)))))
    (doto cancel (.addActionListener (proxy [ActionListener] [] (actionPerformed [event] (onCancel)))))
    
    (let [contentPane (.getContentPane dialog)]
      (let [panel (JPanel.)]      
	(. contentPane add panel BorderLayout/CENTER)
	(let [layout (GroupLayout.  panel)]
	  (. panel setLayout layout)
	  (doto layout
	    (.setAutoCreateGaps true)
	    (.setAutoCreateContainerGaps true))
	  (. layout setHorizontalGroup 
	     (doto (. layout createSequentialGroup)
	       (.addGroup (doto (. layout createParallelGroup (GroupLayout$Alignment/TRAILING))
			    (.addComponent hostLabel)
			    (.addComponent portLabel)))
	       (.addGroup (doto (. layout createParallelGroup)
			    (.addComponent host 200 GroupLayout/DEFAULT_SIZE 200)
			    (.addComponent port 10 GroupLayout/DEFAULT_SIZE 50)))))
	  (. layout setVerticalGroup 
	     (doto (. layout createSequentialGroup)
	       (.addGroup (doto (. layout createParallelGroup (GroupLayout$Alignment/BASELINE))
			    (.addComponent hostLabel)
			    (.addComponent host)))
	       (.addGroup (doto (. layout createParallelGroup (GroupLayout$Alignment/BASELINE))
			    (.addComponent portLabel)
			    (.addComponent port )))))))
      (let [buttonPanel (JPanel.)]
	(. contentPane add buttonPanel BorderLayout/SOUTH)
	(doto buttonPanel 
	  (.setLayout (FlowLayout. FlowLayout/CENTER))
	  (.add ok)
	  (.add cancel))))
    (.pack dialog)
    (. dialog setVisible true)))



      
      
 
(defn connect 
  ([host port]
  (with-open [s (Socket. host port)
	      ois (ObjectInputStream. (.getInputStream s))]
    (let [server (.readObject ois)]
      (let [last @current-server] ;Is it possible to disable this, and clean up 
	(swap! current-server (fn [_] server))))))
  ([frame]
     (connect-dialog frame)
     ))
      
     
(defn exit
  ([]
     (*shutdown*))
  ([frame]
     (when (= JOptionPane/YES_OPTION 
	      (. JOptionPane showConfirmDialog
		 frame 
		 "Are you sure you want to exit?",
		 "Exit process" 
		 JOptionPane/YES_NO_OPTION))
       (.dispose frame)
       (exit))))
(defn new-runtime-panel []
  {:panel (JButton. "Runtime panel")})

(defn new-analysis-panel [] 
  (let [panel (JPanel.)
	table-columns (atom [])
	table-rows (atom [])
	table-model (proxy [AbstractTableModel] []
		      (getRowCount [] (count @table-rows))
		      (getColumnCount [] (count @table-columns))
		      (getValueAt [row column]))
	time-series (TimeSeriesCollection.)]
    (doto panel
      (.setLayout (BorderLayout.))
      (.add (doto (JSplitPane.)
	      (.setOrientation JSplitPane/VERTICAL_SPLIT)
	      (.setName "splitPane")
	      (.setBottomComponent (doto (JScrollPane.)
				   (.setViewportView (doto (JTable.)
						       (.setDefaultRenderer 
							(class Color) 
							(proxy [JLabel TableCellRenderer] []
							  (getTableCellRendererComponent 
							   [table color selected focus row column]
							   (proxy-super setOpaque true)
							   (proxy-super setBackground color))))
						       (.setModel table-model)
						       (.setName "table")
						       (.setAutoCreateRowSorter true)))))
	    (.setTopComponent (doto (ChartPanel. 
				     (doto (ChartFactory/createTimeSeriesChart 
					    nil nil nil 
					    time-series false false false))))))))
    {:panel panel 
     :table-model table-model 
     :time-series time-series}))

(defn get-names [from to]
  (let [names [{:vips "Allan" :vops "Nisse"}
	       {:vips "Arne" :vops "Nils"}
	       {:vips "Allan" :vops "Nils"}
	       {:lul "Dalle"}]]
    (reduce (fn [result name]
	      (reduce (fn [r per-subname] 
			(if-let [this-name (get result (key per-subname))]
			  (assoc r (key per-subname) (conj this-name (val per-subname)))
			  (assoc r (key per-subname) [(val per-subname)])))
		      result 
		      (reduce (fn [a subname] (assoc a (key subname) name)) {} name)))
	       (sorted-map) names)
    ))

(defn get-data [from to names]
  (let [df #(.parse (java.text.SimpleDateFormat. "yyyyMMdd HHmmss") %)]
    {{:vips "Allan" :vops "Nisse"}
     {(df "20010101 010101") 1 (df "20010101 010102") 2 (df "20010101 010103") 3 (df "20010101 010104") 2}})
    )
			
(defn add-analysis [contents]
;  (when-not @current-server
;    (JOptionPane/showMessageDialog 
;     (:panel contents) "Not connected" 
;     "Error" JOptionPane/ERROR_MESSAGE))
  (let [dialog (JDialog. (SwingUtilities/windowForComponent (:panel contents)) "Add" false)
	from (let [model (SpinnerDateModel.)
		   spinner (JSpinner. model)]
	       spinner)
	to (let [model (SpinnerDateModel.)
		 spinner (JSpinner. model)]
	     spinner)
	add (JButton. "Add")
	
	close (JButton. "Close")
	centerPanel (JPanel.)
	components-on-center (atom [])
	combomodels-on-center (atom {})
	combos-on-center (atom [])
	onAdd (fn [] 
		(let [from-date (.getDate (.getModel from))
		      to-date (.getDate (.getModel to))
		      name-values (reduce (fn [result name-combomodel] 
					     (let [the-value (.getSelectedItem (val name-combomodel))]
					       (if (not (= "" the-value))
						 (conj result (key name-combomodel) the-value)
						 result)))
					   [] @combomodels-on-center)]
		  
		(.start (Thread. #(let [result (get-data  from-date
							   to-date
							   name-values)]
				    (SwingUtilities/invokeLater (fn [] (println result )
								  (let [graph (:time-series contents)]
								    (dorun (map (fn [data]
										  (let [time-serie 
											(if-let 
											    [serie 
											     (. graph getSeries (str (key data)))]
											  serie
											  (let [serie (TimeSeries. (str (key data)))]
											    (. graph addSeries serie)
											    serie))]
										    (let [new-timeserie 
											  (reduce (fn [toAdd entry]
												    (.add toAdd (TimeSeriesDataItem. 
														 (Millisecond. (key entry)) 
														 (val entry)))
												    toAdd)
												  (TimeSeries. "") (val data))]
										      (.addAndOrUpdate time-serie new-timeserie))))
										result)))
								  ))) "Data Retriever"))))

	]
    

     (doto dialog
      (.setDefaultCloseOperation WindowConstants/DISPOSE_ON_CLOSE)
      (.setResizable false))
     (.addActionListener close (proxy [ActionListener] [] (actionPerformed [event] (.dispose dialog))))
     (.addActionListener add (proxy [ActionListener] [] (actionPerformed [event] (onAdd))))
     (let [contentPane (.getContentPane dialog)]
       (let [panel (JPanel.)]      
	(. contentPane add panel BorderLayout/NORTH)
	(doto panel
	  (.setLayout (FlowLayout. FlowLayout/CENTER))
	  (.add (JLabel. "From:"))
	  (.add from)
	  (.add (JLabel. "To:"))
	  (.add to)
	  (.add (let [update (JButton. "Update")
		      onUpdate 
		      (fn [] 
			(let[ names (get-names (.. from (getModel) (getDate)) 
					       (.. to (getModel) (getDate)))
			     combo-a (atom nil)
			     combo-action (proxy [ActionListener] []
					    (actionPerformed 
					     [event]
					     (let [requirements 
						   (reduce (fn [requirements keyword-model]
							     (assoc requirements 
							       (key keyword-model) ;Should requirement have name?
							       (.getSelectedItem (val keyword-model)))
							     ) 
							   {} 
							   (filter #(not (= "" (. (val %) getSelectedItem))) @combomodels-on-center))]
					       (dorun (map (fn [combo] (.removeActionListener combo @combo-a)) @combos-on-center))
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
							   @combomodels-on-center))
					       
					       (dorun (map (fn [combo] (.addActionListener combo @combo-a)) @combos-on-center)))))]
			  (swap! combo-a (fn [_] combo-action))
			  
			  (dorun (map #(. centerPanel remove %) @components-on-center))
				  (swap! components-on-center (fn [_] []))
				  (swap! combomodels-on-center (fn [_] {}))
				  (swap! combos-on-center (fn [_] []))
				  (dorun (map (fn [a-name]
						(let [field-name (name (key a-name))
						      label (JLabel. field-name)
						      comboModel (DefaultComboBoxModel.)
						      combo (JComboBox. comboModel)
						      ]
						  (. comboModel addElement "")
						  (let [toadd (reduce (fn [toadd i]
									(if-let [a-value ((key a-name) i)]
									  (conj toadd a-value)
									  toadd))
								      #{} (val a-name))]
						    (dorun (map (fn [el] (. comboModel addElement el)) toadd)))  
						  (. combo setName field-name)
						  (. combo addActionListener combo-action)
						  (. centerPanel add label)
						  (. centerPanel add combo)
						  (swap! components-on-center (fn [l] (conj l label)))
						  (swap! components-on-center (fn [l] (conj l combo)))
						  (swap! combos-on-center (fn [l] (conj l combo)))
						  (swap! combomodels-on-center (fn [l] (assoc l (key a-name) comboModel))))) 
					      names))
				  
				  
				  
				  (.pack dialog)))]
		  (. update addActionListener (proxy [ActionListener] [] (actionPerformed [_] (onUpdate) )))
		  update))))
       (. contentPane add centerPanel BorderLayout/CENTER)
       (let [panel (JPanel.)]
	 (. contentPane add panel BorderLayout/SOUTH)
	 (doto panel
	   (.setLayout (FlowLayout. FlowLayout/CENTER))
	  (.add add)
	  (.add close)))
       )
     (doto dialog
       (.pack)
       (.setVisible true))
))

(defn add-runtime [contents]
(JOptionPane/showMessageDialog 
  (:panel contents) "Not yet implemented" 
 "Error" JOptionPane/ERROR_MESSAGE))

	 
(defn add [contents]
  (if (:time-series contents)
    (add-analysis contents)
    (add-runtime contents)))

(defn new-window [analysis]
  
  (let [frame (JFrame.)
	contents (if analysis (new-analysis-panel) (new-runtime-panel))]
    (swap! frames #(conj % frame))
    (doto frame 
      (.setDefaultCloseOperation (JFrame/DISPOSE_ON_CLOSE))
      (.addWindowListener (proxy [WindowAdapter] []
			   (windowClosed [window-event]
					 (println "closed")
					 (swap! frames #(disj % frame))
					 (proxy-super windowClosed window-event)
					 (when (= 0 (count @frames)) (exit)))))
					 
      (.setTitle "Monitoring Window")
      (.setName "Monitoring Window")
      (.setMinimumSize (Dimension. 300 300)) 
      (doto (.getContentPane) 
	(.setLayout ( BorderLayout.))
	(.add (:panel contents) BorderLayout/CENTER))
      (.setJMenuBar (doto (JMenuBar.)
		      (.add (doto (JMenu. "File")
			      (.setName "fileMenu")
			      (.add (doto (JMenuItem. "New Analysis window")
				      (.setName "newAnalysisWindow")
				      (.addActionListener (proxy [ActionListener] []
							    (actionPerformed [action-event]
									     (new-window true))))))
			      (.add (doto (JMenuItem. "New Runtime window")
				      (.setName "newRuntimeWindow")
				      (.addActionListener (proxy [ActionListener] []
							    (actionPerformed [action-event]
									     (new-window false))))))
			      (.add (doto (JMenuItem. "Connect")
				      (.setName "connectMenu")
				      (.addActionListener (proxy [ActionListener] []
							    (actionPerformed [action-event]
									     (connect frame))))))
			      
			      (.add (doto (JMenuItem. "Add")
				      (.setName "add")
				      (.addActionListener (proxy [ActionListener] []
							    (actionPerformed [action-event]
									     (add contents))))))
			      (.add (doto (JMenuItem. "Close")
				      (.setName "closeWindow")
				      (.addActionListener (proxy [ActionListener] []
							    (actionPerformed [action-event]
									     (.dispose frame))))))
			      (.add (doto (JMenuItem. "Exit")
				      (.setName "exit")
				      (.addActionListener (proxy [ActionListener] []
							    (actionPerformed [action-event]
									     (exit frame))))))
			      ))))
			      
	
      (.pack)
      (.setVisible true))))
