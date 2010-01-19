
import sys
sys.path+=['C:\\Program Files\\IronPython 2.6\\Lib']
import os

from System.Text.RegularExpressions import Regex
from System import Environment
s=r'net use \\mssj009 /USER:mssj009\appadmin %qwe987%'
print s
print (os.system(s))
hosts.Add(Environment.MachineName, Regex("Process"))
hosts.Add("mssj009", Regex("Process"))