(ns se.sj.monitor.gui
  (:import (javax.swing JFrame JButton JOptionPane JMenuBar JMenu JMenuItem))
  (:import (java.awt Dimension BorderLayout))
  (:import (java.awt.event WindowAdapter ActionListener)))

(def frames (atom #{}))

(defn redraw []
     (dorun (map (fn [frame] (.revalidate frame)) @frames)))

(def *shutdown* #(println "Shutdown :)"))
     
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

(defn new-graph-panel [] (JButton. "GraphPanel"))
(defn add [graph-panel]
  (println "Add"))

(defn new-window []
  (let [frame (JFrame.)
	graph-panel (new-graph-panel)]
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
	(.add graph-panel BorderLayout/CENTER))
      (.setJMenuBar (doto (JMenuBar.)
		      (.add (doto (JMenu. "File")
			      (.setName "fileMenu")
			      (.add (doto (JMenuItem. "New window")
				      (.setName "newWindow")
				      (.addActionListener (proxy [ActionListener] []
							    (actionPerformed [action-event]
									     (new-window))))))
			      (.add (doto (JMenuItem. "Add")
				      (.setName "add")
				      (.addActionListener (proxy [ActionListener] []
							    (actionPerformed [action-event]
									     (add graph-panel))))))
			      (.add (doto (JMenuItem. "Close")
				      (.setName "closeWindow")
				      (.addActionListener (proxy [ActionListener] []
							    (actionPerformed [action-event]
									     (.dispose frame))))))
			      (.add (doto (JMenuItem. "Exit")
				      (.setName "exit")
				      (.addActionListener (proxy [ActionListener] []
							    (actionPerformed [action-event]
									     (exit frame))))))))))
			      
	
      (.pack)
      (.setVisible true))))
