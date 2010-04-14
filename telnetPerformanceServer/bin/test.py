
import sys
sys.path+=['C:\\Program Files\\IronPython 2.6\\Lib']
import os
from System.Text.RegularExpressions import Regex
from System import Environment
s=r'net use \\mssj009 /USER:mssj009\appadmin %qwe987%'
os.system(s)
hosts.Add(Environment.MachineName, Regex("Processor|System|Memory|PhysicalDisk|Network Interface"))
#hosts.Add("mssj009", Regex("Processor|System|Memory|PhysicalDisk|Network Interface"))
import clr
clr.AddReference("TelnetPerfmonServer")
from PerformanceTools.TelnetPerfmonServer import Credentials
credentials.Add(Environment.MachineName, Credentials("citrixadm",Environment.MachineName, "citrixadm"))
