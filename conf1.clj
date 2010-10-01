(ns conf1 (:use monitor.monitor))

(java6-jmx)
(perfmon "mssj022" 3434
	 "Processor|System|Memory|PhysicalDisk|Network Interface" 
	 ".*User Time|.*Interrupt Time|.*Processor Time|.*Idle Time|Interrupts/sec|Context Switches/sec|Available Bytes|Current Disk Queue Length|Output Queue Length"
	 ".*")
(perfmon "mssj103" 3434
	 "Processor|System|Memory|PhysicalDisk|Network Interface" 
	 ".*User Time|.*Interrupt Time|.*Processor Time|.*Idle Time|Interrupts/sec|Context Switches/sec|Available Bytes|Current Disk Queue Length|Output Queue Length"
	 ".*")
(java6-jmx "Java2D" "localhost" 3031)
(in-env 60 10 "/home/stefan/testdb" 3030)




