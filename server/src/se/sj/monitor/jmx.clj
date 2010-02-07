(ns se.sj.monitor.jmx
  (:import (javax.management.remote JMXConnectorFactory JMXServiceURL) 
	   (javax.management JMX ObjectName) 
	   (java.lang.management ManagementFactory RuntimeMXBean)
	   (java.util Date)
	   (java.net InetAddress)
	   (java.util.concurrent TimeUnit)
	   (java.text SimpleDateFormat))
					; for testing purposes
  (:use clojure.test)
					;for repl purposes
  (:use (clojure stacktrace inspector)))


(def *current-runtime-bean* (. ManagementFactory getRuntimeMXBean))
(def *vmname* (. *current-runtime-bean* getName))
(def *current-host* (. (. InetAddress getLocalHost) getCanonicalHostName))
(def known-hosts (atom {}))

(defn remote-time 
  ([runtime]
     (let [  remoteTimeMs (+ (. runtime getStartTime) (. runtime getUptime))] 
       (Date. remoteTimeMs)))
  ([]
     (remote-time *current-runtime-bean*)))


(defn remote-uptime
  ([runtime]
     (. runtime getUptime))
  ([runtime timeUnit]
     (. timeUnit convert (remote-uptime runtime) (. TimeUnit MILLISECONDS))))
     
  

(defn remote-pid
  [runtime]
  (let [name (. runtime getName)]
    (. Integer parseInt (second ( re-matches #"([0-9]+)@.*" name)))))

(defn remote-hostname
  []
  *current-host*)

(defn vmname
  []
  *vmname*)

(defn using-jmx 
"Where fun is a fn taking a javax.management.MBeanServer"
  ([host port vmname fun]
     (binding [*vmname* vmname] 
     (let [url  (str "service:jmx:rmi:///jndi/rmi://"  host ":" port "/jmxrmi")]
       (with-open [connector (. JMXConnectorFactory connect 
				(JMXServiceURL. url))]
	 (let [connection (. connector getMBeanServerConnection)]
	   (binding [*current-runtime-bean* (. JMX newMXBeanProxy connection 
					       (ObjectName. "java.lang:type=Runtime") RuntimeMXBean)
		     *current-host* (let [ addr (. InetAddress getByName host)]
				      (if (. addr isLoopbackAddress)
					(. (. InetAddress getLocalHost) getCanonicalHostName)
				      (. addr getCanonicalHostName)))]
	       (fun connection)))))))
  ([port-or-host vmname-or-port fun]
     (if (integer? port-or-host)
       (if (integer? vmname-or-port)
	 (throw (IllegalArgumentException. (str "Both port-or-host and vmname-or-port can't be port."
					       " That is, integers")))
	 (using-jmx *current-host* port-or-host vmname-or-port fun))
       (if (not (integer? vmname-or-port))
	 (throw (IllegalArgumentException. "vmname-or-port has to be port. That is, integer, if port-or-host is host string"))  
       (using-jmx port-or-host vmname-or-port *vmname* fun))))
    
  ([port fun]
     (using-jmx *current-host* port *vmname* fun)))



(defn afun
  [server]
   (let [runtime (. JMX 
		    newMXBeanProxy server 
		    (ObjectName. "java.lang:type=Runtime") RuntimeMXBean)]
     (str *vmname* " "  
      (remote-hostname) " " 
      (remote-pid runtime) " "
      (remote-time) 
      " Uptime:" (remote-uptime runtime TimeUnit/MINUTES) " min")))

;We can identify process name by pid in Process instance and pid in Perfmon Process and match that with the pid from runtime getName


  

