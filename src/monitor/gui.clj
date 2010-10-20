(ns monitor.gui
  (:use (clojure stacktrace))
  (:use (monitor analysis runtime commongui))
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

(def exit-on-shutdown (atom false))

(when (not (= "true" (System/getProperty "stayalive")))
  (reset! exit-on-shutdown true))

(def *shutdown* #(do 
		   (when-let [t @runtime-thread]
		     (swap! runtime-thread (fn [t] nil))
		     (while (.isAlive t)
			    (try
			     (.interrupt t)
			     (.join t)
			     (catch InterruptedException _)))

		     #_(println "runtime thread is terminated"))
		   (when @exit-on-shutdown
		     (System/exit 0))))

(def current-server (atom nil))
(try
 (UIManager/setLookAndFeel (UIManager/getSystemLookAndFeelClassName))
(catch Exception _))

(defn server [] @current-server)
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
    (let [serv (.readObject ois)]
	(swap! current-server (fn [_] serv)))
    (when (not @runtime-thread)
      (swap! runtime-thread (fn [_] (doto (Thread. #(while @runtime-thread
							   (try
							    (Thread/sleep 15000)
							    (get-new-data  server)
							    (catch Exception e
							      (when-not (= InterruptedException (class e))
							      (print-cause-trace e)
							      (println)))))
						   "Runtime Thread")
				      (.setDaemon true))))
      (.start @runtime-thread))))

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



(defn add-analysis [contents]
  (if (not @current-server)
    (JOptionPane/showMessageDialog 
     (:panel contents) "Not connected" 
     "Error" JOptionPane/ERROR_MESSAGE)
  (let [dialog (analysis-add-dialog contents server)]
    (doto dialog
      (.pack)
      (.setVisible true)))))

(defn add-runtime [contents]

 (let [dialog (runtime-add-dialog contents server)]
    (doto dialog
      (.pack)
      (.setVisible true))))

	 
(defn add [contents]
  (if (:time-series contents)
    (add-analysis contents)
    (add-runtime contents)))

(defn new-window [analysis]
  (reset! new-window-fn new-window) 
  (let [frame (JFrame.)
	contents (if analysis (new-analysis-panel) (new-runtime-panel frame))]
    (swap! frames #(conj % frame))
    (doto frame 
      (.setDefaultCloseOperation (JFrame/DISPOSE_ON_CLOSE))
      (.addWindowListener (proxy [WindowAdapter] []
			   (windowClosed [window-event]
					 (swap! frames #(disj % frame))
					 (proxy-super windowClosed window-event)
					 (when (= 0 (count @frames)) (exit)))))
					 
      (.setTitle (:name contents))
      (.setName (:name contents))
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
      (.setVisible true))
    contents))


