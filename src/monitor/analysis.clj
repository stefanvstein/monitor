(ns monitor.analysis
  (:use [clojure stacktrace pprint]
        [monitor commongui tools mem]
        [clojure.contrib profile])

  (:import [javax.swing UIManager JFrame JButton JOptionPane JMenuBar JMenu JMenuItem 
            JPanel JScrollPane JSplitPane JTable JCheckBox JLabel Box JDialog JComboBox
            JTextField WindowConstants JSpinner SpinnerDateModel SwingUtilities
            DefaultComboBoxModel GroupLayout BoxLayout GroupLayout$Alignment JPopupMenu
            BorderFactory JSpinner$DateEditor  DefaultCellEditor Box KeyStroke JComponent]
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
           [org.jfree.chart.event ChartProgressEvent ChartProgressListener RendererChangeEvent]
           [org.jfree.chart ChartFactory ChartPanel JFreeChart ChartUtilities]
           [org.jfree.data.time TimeSeries TimeSeriesCollection TimeSeriesDataItem FixedMillisecond]
           [org.jfree.chart.renderer.xy SamplingXYLineRenderer StandardXYItemRenderer XYSplineRenderer]
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
  


(defn on-add-to-analysis [from to shift name-values func-string
			  { time-series-coll :time-series
			   disable-col :disable-collection
			   enable-col :enable-collection
			   colors :colors
			   ^JFreeChart chart  :chart
			   add-to-table :add-to-table
			   add-column :add-column
			   status-label :status-label
			   data-queue :transfer-queue
			   num-to-render :num-to-render
			   num-rendered :num-rendered
			   name-as-comparable :name-as-comparable}
			  server]
  (let [date-pairs-for-names (fn date-pairs-for-names [from to spec]
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
                                           (assoc r name (filter identity
                                                                 (map (fn [i]
                                                                        (when (contains? (val i) name)
                                                                          (key i)))
                                                                      all-names-per-dates))))
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
				  (. serie setNotify false)
                                  (. time-series-coll addSeries serie)
				  (let [visible-fn (fn ([]
							  (if-let [index (index-by-name time-series-coll identifier)]
							    (.. chart (getPlot) (getRenderer) (getItemVisible index 0 ))
							    false))
						     ([visible]
							(when-let [index (index-by-name time-series-coll identifier)]
							  (.. chart (getPlot) (getRenderer) (setSeriesVisible index visible))
							  )))
					new-index (dec (count (.getSeries time-series-coll)))]
                                    (println "new-index" new-index)
				    (doto (.. chart 
					      (getPlot) (getRenderer))
				      (.setSeriesPaint new-index color)
				      (.setSeriesStroke new-index (BasicStroke. 2.0)))
                                    (add-to-table data-key color visible-fn)
                                    (dorun (map (fn [i] (add-column i)) (keys data-key)))
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

        store-to-disk (fn [keys data])
        get-from-disk (fn [keys]
                        (sorted-map))
        remove-from-disk (fn [keys])
	
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
        
                                        ; Implementera disk
                                    
                                    
                                        ;Implementera abort.
                                        ;Sätt hårdkodad färg på strecket
        update-method (fn update-method [to-update data]
                        (let [update (fn update []
                                       (stop-chart-notify)
                                       (try
                                       (doseq [data data]
                                        ; Få skift time att lägga till shift nyckel också, ok
                                        ;for each data
                                        ; hämta eller skapa timeserie, behövs inte
                                         (let [all-values (merge (get-from-disk (key data)) (val data))
                                               data-key (assoc (key data) :type func-string)]
                                           (store-to-disk  (name-as-comparable data-key) all-values)
                                           (let [calc-data (reduce-samples (with-nans (transform all-values func-string) nan-distance))
                                                 ;(transform all-values func-string)
                                                 ]
                                             (let [time-serie (if-let [serie (. time-series-coll getSeries (name-as-comparable data-key))]
                                                                serie
                                                                (create-new-time-serie data-key))]
                                               (.clear time-serie)
                                               (doseq [d calc-data]
                                                 (.add time-serie (TimeSeriesDataItem.
                                                                   (FixedMillisecond. (key d))
                                                                   ^Number (val d)))))
                                             ))
                                        ; merge innehåll med  motsvarande temp från disk
                                        ;skriv nya temp, i bakgrunden
                                        ;transform
                                        ;skapa nans
                                        ;reducera samples
                                       ; skapa och stoppa in ny serie
                                       (when data
    ;                                     (println "Data retrieved" )
   ;                                      (pprint data)
                                         ;(flush)
                                         ))
                                       (finally (start-chart-notify))))]
                            (if-let [d (first to-update)]
                              (do (future
                                    (let [data (get-data-for (key d) (val d))]
                                      (SwingUtilities/invokeLater (fn [] (update-method (next to-update) data)))))
                                  (update))
                              (do (update)
                                  (println "Done")))))]

          (println "Getting names")
          (future
            (let [to-update (date-pairs-for-names from to name-values)]
              (when (seq to-update)
                (SwingUtilities/invokeLater (fn [] (println "Getting first data")))
                (let [data (get-data-for (key (first to-update)) (val (first to-update)))]
                  (SwingUtilities/invokeLater (fn []
                                                (update-method (next to-update) data)))))))))

(defn on-add-to-analysis-OLD [from to shift name-values func-string
			  { time-series-coll :time-series
			   disable-col :disable-collection
			   enable-col :enable-collection
			   colors :colors
			   ^JFreeChart chart  :chart
			   add-to-table :add-to-table
			   add-column :add-column
			   status-label :status-label
			   data-queue :transfer-queue
			   num-to-render :num-to-render
			   num-rendered :num-rendered
			   name-as-comparable :name-as-comparable}
			  server]

  (let [collection-subscribers (atom [])
	stop-chart-notify (fn stop-chart-notify []
                            (.setNotify chart false)
                            (disable-col))
	start-chart-notify (fn start-chart-notify []
                             (.setNotify chart true)
			     (enable-col))

	shift-time (if (= 0 shift)
		     identity
		     (fn [a] (with-keys-shifted a (fn [d] d) (fn [#^Date d] (Date. (long (+ (.getTime d) shift)))))))
		     
	create-new-time-serie (fn [data-key]
				(binding [*stime* false]
				(stime "create-new-time-serie"
				(let [identifier (name-as-comparable data-key)
				      serie (TimeSeries. identifier)
				      color (colors)]
				  (. serie setNotify false)
				  (stime " addSerie" (. time-series-coll addSeries serie))
;				  (println "Count" (.getSeriesCount graph))
				  (let [visible-fn (fn ([]
							  (if-let [index (index-by-name time-series-coll identifier)]
							    (.. chart (getPlot) (getRenderer) (getItemVisible index 0 ))
							    false))
						     ([visible]
							(when-let [index (index-by-name time-series-coll identifier)]
							  (.. chart (getPlot) (getRenderer) (setSeriesVisible index visible))
							  )))
					new-index (dec (count (.getSeries time-series-coll)))]
				    (doto (.. chart 
					      (getPlot) (getRenderer))
				      (.setSeriesPaint new-index color);(Color. (.getRed color) (.getGreen color) (.getBlue color) 200))
				      (.setSeriesStroke new-index (BasicStroke. 2.0)))
				    
				    
				    (stime " add-to-table" (add-to-table data-key color visible-fn))
				  (stime " add-column" (dorun (map (fn [i] (add-column i)) (keys data-key)))) 
				  serie)))))
	remove-with-empty-cols-as-val (fn [data]
					(let [r (into {} (filter #(seq (val %)) data))]
					  (when (seq r)
					    r)))

	updatechart (fn [data]
;		      (let [d (remove-with-empty-cols-as-val data)]
;			(when d
                      (swap! data-queue conj data)
                      (SwingUtilities/invokeLater
                       
                       (fn []
                                        ;#(.setText status-label (str "Rendering "))
                         

                                
                                  (let [datas (into [] @data-queue)]
                                    (when-not (zero? (count datas))
                                      (swap! data-queue (fn [dq] (vec (drop (count datas) dq))))
                                      (stop-chart-notify)
                                      (try
                                        ;(doseq [data datas]
                                        (let [dat (last datas)
                                              data (reduce (fn [r e]
                                                            (assoc r (key e) (transform (val e) func-string)))
                                                          {}
                                                          dat)]

                                        (dorun (map (fn [data]
                                                      (let [data-key (let [dk (assoc (key data) :type func-string)]
								(if (= 0 shift)
								  dk
								  (let [df (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")]
								    (assoc dk :shifted (str (.format df from) " to " (.format df
															      (Date. (long (+ (.getTime ^Date from) shift)))))))))
						     ;make-double-values (fn [e] (into (sorted-map) (map #(first {(key %) (double (val %))}) e)))
											      data-values (val data) #_(make-double-values (val data))    ;???????

						     ^TimeSeries time-serie (if-let [serie (. time-series-coll getSeries (name-as-comparable data-key))]
									      serie
									      (create-new-time-serie data-key))
						     data-from-serie (binding [*stime* false] (stime "time-serie-to-sorted-map" (time-serie-to-sortedmap time-serie))) ; use last-from-timeserie and keep the old in the time serie
						     data-with-new-data (binding [*stime* false] (stime "merge with new" (merge data-from-serie data-values)))
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
					;reduced-samples (reduce-samples data-with-nans)
						     reduced-samples data-with-nans
						     
						     temp-serie (doto (TimeSeries. "") (.setNotify false))
						     new-serie (binding [*stime* false] (stime "fill serie" (reduce (fn [^TimeSeries r i]
									 (.add r  (TimeSeriesDataItem. 
										   (FixedMillisecond. (key i)) ;Fixed is better than Millisecond
										   ^Number (val i)))
									 r)
								       temp-serie reduced-samples)))]
						 
						    
						 (.clear time-serie)
						 (binding [*stime* false] (stime "addAndOrUpdate" (.addAndOrUpdate time-serie new-serie)))))
					       data)))

				  
		       (catch Exception e
			 (.printStackTrace e))
		       (finally (start-chart-notify))))))))]
    
    (future
     
      
      (try
	(SwingUtilities/invokeLater (fn []
				      (.setText status-label "Retrieving...")))
	(try
					;	(if (= "Raw" func-string)
          (let [days (full-days-between from to)
                alldata (atom {})]
            (swap! num-to-render (fn [d] (+ d (count days))))


            (doseq [e days]
              (doseq [nv (according-to-specs (reduce (fn [r e]
                                                       (reduce (fn [r e]
                                                                 (conj r e)) r e))
                                                     #{} (vals (grouped-maps (get-names (first e) (second e) server))))
                                             [(apply hash-map name-values)])]
                            (println "get" nv e)
                            (swap! alldata (fn [a] (reduce (fn [r l]
                                                             (if (seq (val l))
                                                               (if-let [current (get r (key l))]
                                                                 (assoc r (key l) (merge current (val l)))
                                                                 (assoc r (key l) (val l)))
                                                               r))
                                                           a
                                                           (shift-time (get-data (first e)
                                                                                 (second e)
                                                                                 (reduce #(conj %1 (key %2) (val %2)) [] nv)
                                                                                server)))))
                       
                            (updatechart @alldata))))
	(finally
	(SwingUtilities/invokeLater (fn []
;				      (when (= @num-rendered @num-to-render)
					(reset! num-rendered 0)
					(reset! num-to-render 0)
					(.setText status-label " ")))))
	
	(update-chart chart)
	
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

  (let [status-label (JLabel. " ")
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
										     (.invalidateLayout layout p)))))))))

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
		
	


	tbl-model (create-table-model
		   (fn [row] 
		     (.removeSeries time-series (.getSeries time-series (name-as-comparable row))))
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
					      (.setSeriesStroke renderer row (BasicStroke. 4.0)) 
					      (reset! highlighted [row stroke]))))
			      
			      (mouseExited [event]
					   (when-let [hl @highlighted]
					     (let [renderer (.. chart (getPlot) (getRenderer))]
					       (.setSeriesStroke renderer (first hl) (second hl))
											       (reset! highlighted nil)) 
					     )))
	]
					(.setForegroundAlpha (.getPlot chart) 0.8)
					(.setDrawSeriesLineAsPath (.getRenderer (.getPlot chart)) true)
;					(.setRangeCrosshairLockedOnData (.getPlot chart) false)
    ;(.setRenderer (.getPlot chart) (XYSplineRenderer. 1))

		(.addProgressListener chart (proxy [ChartProgressListener] []
					      (chartProgress [e]
							     (when (= (.getType e) ChartProgressEvent/DRAWING_FINISHED)
							       (when-let [date (let [l (long (.getDomainCrosshairValue (.getPlot chart)))]
									   (if (zero? l)
									     nil
									     (Date. l)))]
							   (.setText status-label (.format (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss") date))
							   (doseq [s (.getSeries time-series)]
							     (let [index  (let [ind (.getIndex s (FixedMillisecond. date))]
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
								  "")))))))))

							
		

    
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
									     )))))))) BorderLayout/CENTER))
;     (let [d (.getComparator (.getRowSorter table) 2)]
;					      (println d)
;					      (println (.compare d 3 2)))
    (let [show-column (.getColumn (.getColumnModel table) 1)]
      (.setCellEditor show-column (DefaultCellEditor. (JCheckBox.))))
    
					;(ChartUtilities/applyCurrentTheme chart)
   
    {
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
     :transfer-queue (atom [])
     :num-to-render (atom 0)
     :num-rendered (atom 0)
     }))

