(ns se.sj.monitor.gui
  (:use (clojure stacktrace))
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

(def frames (atom #{}))

(defn redraw []
     (dorun (map (fn [frame] (.revalidate frame)) @frames)))

(def *shutdown* #(println "Shutdown :)"))
(def current-server (atom nil))
(try
 (UIManager/setLookAndFeel (UIManager/getSystemLookAndFeelClassName))
(catch Exception _))

(declare connect)
(def lastHost (atom ""))
(def lastPort (atom ""))
(defn connect-dialog [ parent]
  (let [dialog (JDialog. parent "Connection" false)
	ok (JButton. "Ok")
	cancel (JButton. "Cancel")
	host (JTextField. @lastHost)
	port (JTextField. @lastPort)
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
			    (swap! lastHost (fn [_] host))
			    (swap! lastPort (fn [_] (Integer/toString port)))
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
			(do (JOptionPane/showMessageDialog 
			 dialog (.getMessage cause) "Error" JOptionPane/ERROR_MESSAGE)
			    (println cause)))))))
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
	(swap! current-server (fn [_] server)))))

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

(def colors [Color/black Color/red Color/blue Color/green Color/yellow Color/cyan Color/magenta Color/orange Color/pink (Color. 205 133 063) Color/darkGray (Color. 144 238 144) (Color. 139 0 0) (Color. 139 0 139) (Color. 205 104 057) (Color. 192 255 062) (Color. 238 213 210) (Color. 255 215 000) (Color. 239, 222, 205) (Color. 120, 219, 226) (Color. 135, 169, 107) (Color. 159, 129, 112) (Color. 172, 229, 238) (Color. 162, 162, 208) (Color. 206, 255, 29) (Color. 205, 74, 76) (Color. 142, 69, 133) (Color. 113, 75, 35)(Color. 205, 197, 194) ])

(defn new-analysis-panel [] 
  (let [panel (JPanel.)
	color-cycle (atom (cycle colors)) 
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

	time-series (TimeSeriesCollection.)
	chart (ChartFactory/createTimeSeriesChart 
					      nil nil nil 
					      time-series false false false)
	remove-row (fn [row] 
		     (.removeSeries time-series row)
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
		     ;recolor 
		     
		     (dotimes [n (count @table-rows)]
				   (.. chart (getPlot) (getRenderer) (setSeriesPaint n (:color (get @table-rows n))))))
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
						       (.setModel table-model)
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
     :add-to-table add-row
     :add-column add-column
     :chart chart
     :colors (fn [] (first (swap! color-cycle (fn [cyc] (rest cyc)))))
     :time-series time-series}))

(defn- names-as-keyworded [names]
  (reduce (fn [result name]
	    (assoc result (keyword (key name)) (val name))) 
	  {} names))

(defn get-names [from to]
  (let [raw-names (.rawNames @current-server from to)
	names (reduce (fn [result a-map]
			(conj result (names-as-keyworded a-map))) [] raw-names)]
    (reduce (fn [result name]
	      (reduce (fn [r per-subname] 
			(if-let [this-name (get result (key per-subname))]
			  (assoc r (key per-subname) (conj this-name (val per-subname)))
			  (assoc r (key per-subname) [(val per-subname)])))
		      result 
		      (reduce (fn [a subname] (assoc a (key subname) name)) {} name)))
	       (sorted-map) names)))

(defn get-data [from to names]
  (let [stringed-names (interleave 
			(map #(name (first %)) (partition 2 names)) 
			(map #(second %) (partition 2 names)))
	data (.rawData @current-server from to (java.util.ArrayList. stringed-names))]
    (reduce (fn [result a-data] 
	      (assoc result (names-as-keyworded (key a-data)) (val a-data))) 
	    {} data)))

(defn- on-add-to-analysis [from to name-values graph colors chart add-to-table add-column]
  (.start (Thread. #(let [result (get-data from
					   to
					   name-values)]
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

(defn- on-update [names combomodels-on-center centerPanel add-button]
  (let [
;	components-on-center (atom [])
	combos-on-center (atom [])
	placeholder-for-combo-action (atom nil)
	combo-action (proxy [ActionListener] []
		      (actionPerformed 
		       [event]
		       (let [requirements 
			     (reduce (fn [requirements keyword-model]
				       (assoc requirements 
					 (key keyword-model) 
					 (.getSelectedItem (val keyword-model)))
				       ) 
				     {} 
				     (filter #(not (= "" (. (val %) getSelectedItem))) @combomodels-on-center))]
			 (dorun (map (fn [combo] (.removeActionListener combo @placeholder-for-combo-action)) @combos-on-center))
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
			 
			 (dorun (map (fn [combo] (.addActionListener combo @placeholder-for-combo-action)) @combos-on-center))
			 (dorun (map (fn [combo] (if (= 1 (.getItemCount combo))
						   (.setEnabled combo false)
						   (.setEnabled combo true))) 
				     @combos-on-center))
			 (if (every? (fn [model]  (= "" (.getSelectedItem model))) (vals @combomodels-on-center))
			   (.setEnabled add-button false)
			   (.setEnabled add-button true)))))]
    (swap! placeholder-for-combo-action (fn [_] combo-action))
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
		      (. combo addActionListener combo-action)
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
		  names)))))

(defn- add-dialog [contents]
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
			(:add-column contents))))
	
	add (let [add (JButton. "Add")]
	      (.setEnabled add false)
	      add)
	close (JButton. "Close")
	centerPanel (JPanel.)]

    (doto dialog
      (.setDefaultCloseOperation WindowConstants/DISPOSE_ON_CLOSE)
      (.setResizable true))

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
				   (let [names (get-names from to)]
				     (on-update names combomodels-on-center centerPanel add)
				     (.pack dialog))))]
		  (. update addActionListener (proxy [ActionListener] [] (actionPerformed [_] (onUpdate) )))
		  update)))))
    dialog))
			
(defn add-analysis [contents]
  (if (not @current-server)
    (JOptionPane/showMessageDialog 
     (:panel contents) "Not connected" 
     "Error" JOptionPane/ERROR_MESSAGE)
  (let [dialog (add-dialog contents)]
    (doto dialog
      (.pack)
      (.setVisible true)))))

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
