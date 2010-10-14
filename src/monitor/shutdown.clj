(ns monitor.shutdown
  (:import [javax.swing JFrame JPanel JLabel JButton SwingUtilities JOptionPane WindowConstants])
  (:import [java.awt.event ActionListener WindowAdapter])
  (:import [java.awt Frame])
  (:import [java.lang.management ManagementFactory])
  (:import [javax.management ObjectName])
  (:use [monitor termination]))

(def shutdown-frame (atom nil))

(defn shutdown-jmx [text]
  
  (.registerMBean (ManagementFactory/getPlatformMBeanServer)
		  (proxy [monitor.ShutdownMXBean] []
		    (shutdown []
			      (do
				(stop)
				(when @shutdown-frame
				  (.dispose @shutdown-frame)))))
		    (ObjectName. "monitor.man:type=State")))
  
 
					
(defn shutdown-button [text]
  (let [frame (JFrame. text)
	label (JLabel. text)
	button (JButton. "Shutdown")
	shutdown-listener (proxy [ActionListener] []
			    (actionPerformed [a]
					     (if (= JOptionPane/YES_OPTION
						    (JOptionPane/showOptionDialog
						     frame
						     (str "Are you sure you want to shutdown " text "?") "Are you sure?"
						     JOptionPane/YES_NO_OPTION
						     JOptionPane/QUESTION_MESSAGE nil nil nil))
					       (do (stop)
						   (.dispose frame)))))]
    (reset! shutdown-frame frame) 
    (.add frame (doto (JPanel.)
	  (.add label)
	  (.add button)
	  ))
	(.addActionListener button shutdown-listener)
	(.setDefaultCloseOperation frame WindowConstants/DO_NOTHING_ON_CLOSE)
	(.addWindowListener frame (proxy [WindowAdapter] []
				    (windowClosing [_] (.setState frame Frame/ICONIFIED))))

	(.pack frame)
	(SwingUtilities/invokeLater (fn [] (.show frame)))))
    