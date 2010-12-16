(ns monitor.jmx
  (:import (javax.management.remote JMXConnectorFactory JMXServiceURL) 
	   (javax.management JMX ObjectName MBeanServerFactory) 
	   (java.lang.management ManagementFactory RuntimeMXBean MemoryMXBean ThreadMXBean)
	   (java.util Date)
	   (java.net InetAddress UnknownHostException)
	   (java.util.concurrent TimeUnit)
	   (java.text SimpleDateFormat))
  (:use clojure.contrib.logging)
					; for testing purposes
  (:use clojure.test)
					;for repl purposes
  (:use (clojure stacktrace inspector)))



(def *current-mbean-connection* (let [server (ManagementFactory/getPlatformMBeanServer)
				      runtime (ManagementFactory/getRuntimeMXBean)
				      memory (ManagementFactory/getMemoryMXBean)]
				  (assoc {} 
				    :server server 
				    :host (. (. InetAddress getLocalHost) getCanonicalHostName)
				    :runtime runtime
				    :threads (ManagementFactory/getThreadMXBean)
				    :memory memory
				    :vmname "Monitor")));(.getName runtime))))

(def mbean-connections (atom #{*current-mbean-connection*}))


(defn open-mbean-connector
  [mbean-connector]
   (when-let [connector (:connector mbean-connector)]
     (try (.connect connector)
     (let [server (.getMBeanServerConnection connector)
	   result (assoc mbean-connector
		    :runtime  (. JMX newMXBeanProxy server 
				 (ObjectName. "java.lang:type=Runtime") RuntimeMXBean)
		    :memory (. JMX newMXBeanProxy server (ObjectName. "java.lang:type=Memory") MemoryMXBean)
		    :threads (. JMX newMXBeanProxy server (ObjectName. "java.lang:type=Threading") ThreadMXBean)
		    :server server)]
       (swap! mbean-connections (fn [current] (conj current result)))
       result)
     (catch Exception e 
       (info (str "Could not connect to MBeanServer at " 
		  (:host mbean-connector) ":" (:port mbean-connector) "." 
		  ))
       nil))))


(declare remote-hostname vmname)

(defn create-mbean-connector
  "Returns a structure used to connect to a jmx server"
  ([host port vmname]
  (let [url (str "service:jmx:rmi:///jndi/rmi://"  host ":" port "/jmxrmi")
	connector  (. JMXConnectorFactory newJMXConnector (JMXServiceURL. url) nil)]
	 (assoc {} 
		 :vmname vmname
		 :connector connector
		 :port port
		 :host (try 
			(let [ addr (. InetAddress getByName host)]
			  (if (. addr isLoopbackAddress)
			    (. (. InetAddress getLocalHost) getCanonicalHostName)
			    (let [n (. addr getCanonicalHostName)]
			      (if (re-matches #"([0-9]+\.){3}[0-9]+" n)
				(do (println "Here you are")(. addr getHostName))
				n)
			      )))
			(catch UnknownHostException _ host)))))
  ([port-or-host vmname-or-port]
     (if (integer? port-or-host)
       (if (integer? vmname-or-port)
	 (throw (IllegalArgumentException. (str "Both port-or-host and vmname-or-port can't be port."
						" That is, integers")))
	 (create-mbean-connector (remote-hostname) port-or-host vmname-or-port))
       (if (not (integer? vmname-or-port))
	 (throw (IllegalArgumentException. "vmname-or-port has to be port. That is, integer, if port-or-host is host string"))  
	 (create-mbean-connector port-or-host vmname-or-port (vmname)))))
  ([port]
     (create-mbean-connector (remote-hostname) port (vmname))))


(defn adress-of-mbean-connection 
  ([connection]
  (when-let [port (:port connection)]
    {:port port :host (:host connection)}))
  ([]
     (adress-of-mbean-connection 
      *current-mbean-connection*))) 

(defn closed? 
  ([connection]
     (not (:server connection)))
  ([]
     (closed? *current-mbean-connection*)))

(defn close-mbean-connection 
  ([connection]
     (when-let [connector (:connector connection)]
       (try (.close connector) (catch Exception e))
       (let [cleaned (reduce #( assoc %1 (key %2) (val %2))
			     {} 
			     (filter #(some #{:host :connector :vmname :port} %) connection))]
   
	 (swap! mbean-connections (fn [current] 
				    (if (get current connection)
				      (conj (disj current connection) cleaned)
				      current)))
	 cleaned)))

  ([] (close-mbean-connection *current-mbean-connection*)))

(defn forget-about-mbean-connection-matching [requirement]
  (swap! mbean-connections (fn [current]
			     (let [matching (reduce 
					     (fn [result con] 
					       (if (every? #(= (get requirement %) (get con %))
							   (keys requirement))
						 (conj result con)
						 result)) [] current)]
			       (apply disj current matching)))))

(defn remote-time 
  ([connection]
     (when-let [#^RuntimeMXBean runtime (:runtime connection)]
       (Date. (+ (. runtime getStartTime) (. runtime getUptime)))))
([]
   (remote-time *current-mbean-connection*)))
  
(defn remote-memory
  ([]
     (remote-memory *current-mbean-connection*))
  ([con]
     (when-let [#^MemoryMXBean memory (:memory con)]
       {:heap (.getHeapMemoryUsage memory)
	:non-heap (.getNonHeapMemoryUsage memory)})))

(defn remote-threads
  ([]
     (remote-threads *current-mbean-connection*))
  ([connection]
     (when-let [#^ThreadMXBean threads (:threads connection)]
       (.getThreadCount threads))))

(defn remote-uptime
  ([]
     (remote-uptime *current-mbean-connection*))
  ([connection-or-timeunit]
     (if (associative? connection-or-timeunit)
       (when-let [#^RuntimeMXBean runtime (:runtime connection-or-timeunit)]
	 (.getUptime runtime))
       (remote-uptime *current-mbean-connection* connection-or-timeunit)))
  ([connection #^TimeUnit timeUnit]
     (. timeUnit convert (remote-uptime connection) (. TimeUnit MILLISECONDS))))
     
(defn mbean-server 
  ([connection] 
     (:server connection))
  ([] 
     (mbean-server *current-mbean-connection*)))

(defn remote-pid
  ([connection]
     (when-let [ #^RuntimeMXBean runtime (:runtime connection)]
       (let [name (. runtime getName)]
	 (try
	  (. Integer parseInt (second ( re-matches #"([0-9]+)@.*" name)))
	  (catch NumberFormatException _)))))
  ([]
     (remote-pid *current-mbean-connection*)))


(defn remote-hostname
  ([]
     (remote-hostname *current-mbean-connection*))
  ([connection]
     (:host connection)))

(defn vmname
  ([]
     (vmname *current-mbean-connection*))
  ([connection]
     (:vmname connection)))

(defmacro using-jmx-connection 
  [connection & form]
     `(binding [*current-mbean-connection* ~connection]
	~@form))

(defmacro using-jmx
  [host port & form]
  `(let [con# (create-mbean-connector ~host ~port)
	 opened# (open-mbean-connector con#)]
     (if opened#
       (try
	 (binding [*current-mbean-connection* opened#]
	   ~@form)
	 (finally (close-mbean-connection opened#))))))

(defmacro using-named-jmx
  [host port name & form]
  `(let [con# (create-mbean-connector ~host ~port ~name)
	 opened# (open-mbean-connector con#)]
     (if opened#
    (try
     (binding [*current-mbean-connection* opened#]
       ~@form)
     (finally (close-mbean-connection opened#))))))

(defmacro using-named-jmx-port
  [port name & form]
  `(let [con# (create-mbean-connector ~port ~name)
	 opened# (open-mbean-connector con#)]
     (if opened#
    (try
     (binding [*current-mbean-connection* opened#]
       ~@form)
     (finally (close-mbean-connection opened#))))))

(defmacro using-jmx-port
  [port & form]
  `(let [con# (create-mbean-connector ~port)
	 opened# (open-mbean-connector con#)]
     (if opened#
    (try
     (binding [*current-mbean-connection* opened#]
       ~@form)
     (finally (close-mbean-connection opened#))))))



(deftest using-local-jmx
  (is (= (.getName (. JMX newMXBeanProxy (:server *current-mbean-connection*)
		      (ObjectName. "java.lang:type=Runtime") RuntimeMXBean))
	 (.getName (ManagementFactory/getRuntimeMXBean )))))


(deftest using-another-local-jmx
  (if-let [jmxport (System/getProperty "com.sun.management.jmxremote.port")]
    (let [port (Integer/parseInt jmxport)
	  connector (create-mbean-connector port "silly")
	  opened (open-mbean-connector connector)]
      (try
       (is (= 2 (count @mbean-connections)))
       (is (= (.getName (ManagementFactory/getRuntimeMXBean )) 
	      (.getName (:runtime opened))))
       (is (not (= (vmname) (vmname opened))))
       (using-jmx-connection opened
		  (is (= (vmname) (vmname opened))))
       (finally
	(closed? (close-mbean-connection opened ))))
      (using-named-jmx-port port "silly"
		 (is (= (vmname) (vmname opened))))

      (is (= 2 (count (filter #(closed? %) @mbean-connections))))
      (forget-about-mbean-connection-matching {:port port})
      (is (= 0 (count (filter #(closed? %) @mbean-connections))))
      (is (= 1 (count @mbean-connections)))
      (is (= port (:port (adress-of-mbean-connection opened)))))
     
    
    
    (throw (IllegalStateException. "jmx port is not defined" ))))




;We can identify process name by pid in Process instance and pid in Perfmon Process and match that with the pid from runtime getName


  
