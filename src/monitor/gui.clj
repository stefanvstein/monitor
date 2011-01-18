(ns monitor.gui
  (:use [clojure.stacktrace :only [root-cause]])
  (:use [monitor.analysis :only [analysis-add-dialog new-analysis-panel]])
  (:use [monitor.runtime :only [runtime-add-dialog new-runtime-panel get-new-data]])
  (:use [monitor.commongui :only [new-window-fn]])
  (:import (javax.swing UIManager JFrame JButton JOptionPane JMenuBar JMenu JMenuItem 
			JPanel JLabel JDialog JTextField WindowConstants SwingUtilities
			GroupLayout GroupLayout$Alignment))
  (:import (java.awt Dimension BorderLayout FlowLayout))
  (:import (java.awt.event WindowAdapter ActionListener))
  (:import (java.net Socket UnknownHostException ConnectException))
  (:import (java.io ObjectInputStream))
  (:import (java.util.concurrent Executors TimeUnit)))


(def exit-on-shutdown (not (= "true" (System/getProperty "stayalive"))))

(try
  (when-not (System/getProperty "java.awt.headless")
;    (UIManager/setLookAndFeel com.sun.java.swing.plaf.nimbus.NimbusLookAndFeel)
    (UIManager/setLookAndFeel (UIManager/getSystemLookAndFeelClassName))
    )
  (catch Exception _))


(def is-shuttingdown (ref false))
(def frames-and-content (atom {}))

(defn- connect [host port]
  (let [port (if (= (class port) String)
	       (Integer/valueOf port)
	       port)]
    (with-open [s (Socket. host port)
		ois (ObjectInputStream. (.getInputStream s))]
      (.readObject ois))))

(let [current-server (ref nil)
      prefs (java.util.prefs.Preferences/userRoot)
      last-host (ref (.get prefs "last-monitor-host" ""))
      last-port (ref (.get prefs "last-monitor-port" ""))
      connection-string (ref "Not connected")
      runtime-thread (atom nil)
      alive (fn alive [] (let [rt @runtime-thread]
			   (not (or (nil? rt)
				    (.isShutdown rt)
				    (.isTerminated rt)))))]

  
  (defn- update-connection-string []
    (doseq [content (vals @frames-and-content) constr [@connection-string]]
      (SwingUtilities/invokeLater (fn []
      (when-let [connection-label (:connection-label content)]
	(.setText connection-label constr))))))

  (defn- poll []
    (if-let [server @current-server]
      (try
	(get-new-data (fn [] server))
	(catch Exception e
	  (dosync
	   (ref-set connection-string (str "Connection lost " @last-host ":" @last-port)) 
	   (ref-set current-server nil))
	  (update-connection-string)))
      (try
	(if-not @is-shuttingdown
	(when-let [server (connect @last-host @last-port)]
	  (dosync (ref-set current-server server)
		  (ref-set connection-string (str "Connected " @last-host ":" @last-port)))
	  (update-connection-string)))
	(catch Exception _))))
  
  (defn shutdown []
    (when exit-on-shutdown
      (System/exit 0))
    (dosync (ref-set current-server nil)
	    (ref-set is-shuttingdown true))
    (when @runtime-thread
      (.shutdown @runtime-thread)
      (while (.isTerminated @runtime-thread)
	(try
	  (.awaitTermination 1 TimeUnit/SECONDS)
	  (catch Exception _))))
    (dosync (ref-set is-shuttingdown false))
    )

  (defn server [] @current-server)

  (defn schedule [a-fn seconds]
    (when (alive)
      (.schedule @runtime-thread a-fn seconds TimeUnit/SECONDS)))

  
  (defn connect-dialog [parent]
    (let [dialog (doto (JDialog. parent "Connection" false)
		   (.setDefaultCloseOperation WindowConstants/DISPOSE_ON_CLOSE)
		   (.setResizable false))
	
	  host (JTextField. @last-host)
	  port (JTextField. @last-port)
	  on-ok #(let [host (.getText host)
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
			   (when-let [server (connect host port)]
			     (dosync
			      (ref-set current-server server)
			      (ref-set last-host host)
			      (alter last-port (fn [_] (Integer/toString port)))
			      (ref-set connection-string (str "Connected " @last-host ":" @last-port)))
			     (update-connection-string)
			     (.put prefs "last-monitor-host" host)
			     (.put prefs "last-monitor-port" (Integer/toString port))
			     (swap! runtime-thread (fn [rt]
						     (if-not (alive)
						       (doto (Executors/newSingleThreadScheduledExecutor)
							 (.scheduleAtFixedRate poll 1 15 TimeUnit/SECONDS))
						       rt)))
    			     (.dispose dialog)))))
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
				  dialog (.getMessage cause) "Error" JOptionPane/ERROR_MESSAGE)))))))
	  on-cancel #(.dispose dialog)
	  ok (doto (JButton. "Ok")
	       (.addActionListener (proxy [ActionListener] [] (actionPerformed [event]  (on-ok)))))
	  cancel (doto (JButton. "Cancel")
		   (.addActionListener (proxy [ActionListener] [] (actionPerformed [event] (on-cancel)))))
	  host-label (JLabel. "Host")
	  port-label (JLabel. "Port")]
      
      
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
			      (.addComponent host-label)
			      (.addComponent port-label)))
		 (.addGroup (doto (. layout createParallelGroup)
			      (.addComponent host 200 GroupLayout/DEFAULT_SIZE 200)
			      (.addComponent port 10 GroupLayout/DEFAULT_SIZE 50)))))
	    (. layout setVerticalGroup 
	       (doto (. layout createSequentialGroup)
		 (.addGroup (doto (. layout createParallelGroup (GroupLayout$Alignment/BASELINE))
			      (.addComponent host-label)
			      (.addComponent host)))
		 (.addGroup (doto (. layout createParallelGroup (GroupLayout$Alignment/BASELINE))
			      (.addComponent port-label)
			      (.addComponent port )))))))
	(let [buttonPanel (JPanel.)]
	  (. contentPane add buttonPanel BorderLayout/SOUTH)
	  (doto buttonPanel 
	    (.setLayout (FlowLayout. FlowLayout/CENTER))
	    (.add ok)
	    (.add cancel))))
      (.pack dialog)
      (. dialog setVisible true)))

(defn exit
  ([]
     (shutdown))
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
  (if (not (server))
    (JOptionPane/showMessageDialog 
     (:panel contents) "Not connected" 
     "Error" JOptionPane/ERROR_MESSAGE)
  (let [dialog (analysis-add-dialog contents server)]
    (doto dialog
      (.pack)
      (.setVisible true)))))

(defn add-runtime [contents]
  (if (not (server))
    (JOptionPane/showMessageDialog 
     (:panel contents) "Not connected" 
     "Error" JOptionPane/ERROR_MESSAGE)
 (let [dialog (runtime-add-dialog contents server)]
    (doto dialog
      (.pack)
      (.setVisible true)))))

	 
(defn add [contents]
  (if (:time-series contents)
    (add-analysis contents)
    (add-runtime contents)))

(defn new-window [analysis]
  (reset! new-window-fn new-window) 
  (let [frame (JFrame.)
	contents (if analysis (new-analysis-panel) (new-runtime-panel frame))]
    (when-let [connection-label (:connection-label contents)]
      (.setText connection-label @connection-string))
    (swap! frames-and-content #(assoc % frame contents))
    (doto frame 
      (.setDefaultCloseOperation (JFrame/DISPOSE_ON_CLOSE))
      (.addWindowListener (proxy [WindowAdapter] []
			   (windowClosed [window-event]
					 (swap! frames-and-content #(dissoc % frame))
					 (proxy-super windowClosed window-event)
					 (when (= 0 (count @frames-and-content)) (exit)))))
					 
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
									     (connect-dialog frame))))))
			      
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
    contents)))
