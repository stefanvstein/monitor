(ns conf (:use monitor.monitor))
(let [perfmon-general-categories "Processor|System|Memory|PhysicalDisk|Network Interface"
      perfmon-general-counters "User Time|Interrupt Time|Processor Time|Idle Time|Interrupts/sec|Context Switches/sec|Available Bytes|Modified Page List Bytes|Queue Length|Bytes Received|Bytes Sent|Packets Received/sec|Packets Sent/sec|Discarded|Errors|System Cache Resident Bytes|Page Input/sec"
      perfmon-general-instances ""
      perfmon-process-category "Process"
      perfmon-process-counters "Working Set$|Working Set - Private|Private|Thread|% Processor|% User|Handle|Page Faults"
      perfmon-process-instances "java"
      hosts {:local "localhost"
	     :other "127.0.0.1"}
      third  #(when (< 2 (count %)) 
		(nth % 2))]

  ;Monitor jmx on this server
(java6-jmx)


(doseq [s #{["JavaApp" (:local hosts) 6771]
	   ["JavaApp2" (:local hosts) 6772]}]
  (java6-jmx (first s)
	     (second s)
	     (third s)))

(doseq [s #{[(:local hosts) 3002]}]
  (remote-jmx (first s)
	      (second s)))

(doseq [s #{[(:local hosts) 3003]
	   [(:other hosts) 3003]}]
  (linux-proc (first s)
	      (second s)))

(doseq [s #{[(:local hosts) 3434]}]
  (perfmon (first s)
	   (second s)
	   perfmon-general-categories
	   perfmon-general-counters
	   perfmon-general-instances)

  (perfmon (first s)
	   (second s)
	   perfmon-process-category
	   perfmon-process-counters
	   perfmon-process-instances))

(in-env 60
	10
	"db"
	3030))


