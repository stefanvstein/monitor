(ns conf (:use monitor.monitor))

(java6-jmx)
#_(java6-jmx "monitor" "localhost" 3001)
#_(remote-jmx "localhost" 3002)
#_(linux-proc "localhost" 3003)
#_(perfmon "localhost" 3434 "Processor|System|Memory|PhysicalDisk|Network Interface"  
                            ".*User Time|.*Interrupt Time|.*Processor Time|.*Idle Time|Interrupts/sec|Context Switches/sec|Available Bytes|Current Disk Queue Length|Output Queue Length"
                            ".*")
(in-env 60 10 "db" 3030)
