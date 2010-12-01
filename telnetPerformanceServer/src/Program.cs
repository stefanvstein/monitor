using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ServiceProcess;
using System.Threading;
using System.Configuration.Install;
using System.ComponentModel;
using System.Reflection;

namespace PerformanceTools.TelnetPerfmonServer
{
    
    [RunInstaller(true)]
    public class TelnetPerfmonServiceInstaller : System.Configuration.Install.Installer
    {
       

        public TelnetPerfmonServiceInstaller()
        {
            ServiceProcessInstaller process = new ServiceProcessInstaller();
            process.Account = ServiceAccount.LocalSystem;
            ServiceInstaller serviceAdmin = new ServiceInstaller();
            serviceAdmin.StartType = ServiceStartMode.Automatic;
            serviceAdmin.ServiceName = TelnetPerfmonService.name;
            serviceAdmin.DisplayName = "Telnet Perfmon Service";
            serviceAdmin.Description = "Provides a telnet interface for Perfmon";
            Installers.Add(process);
            Installers.Add(serviceAdmin);
        }
    }
    public class TelnetPerfmonService : ServiceBase
    {
        public static readonly String name = "TelnetPerfmonService";
        public TelnetPerfmonService()
        {
        
        }
           
        
        static void Main(String[] args)
        {
            if (args != null && args.Length == 1 && args[0].Length > 1
               && (args[0][0] == '-' || args[0][0] == '/'))
            {
                switch (args[0].Substring(1).ToLower())
                {
                    default:
                        Console.WriteLine("Huh?!");
                        break;
                    case "uninstall":
                    case "u":
                        {
                            ServiceController[] scServices = ServiceController.GetServices();
                            foreach (ServiceController sc in scServices)
                                if (sc.ServiceName == name)
                                    if (sc.Status != ServiceControllerStatus.Stopped && sc.Status != ServiceControllerStatus.StopPending)
                                        sc.Stop();
                            ManagedInstallerClass.InstallHelper(new string[] { "/u", Assembly.GetExecutingAssembly().Location });
                            break;
                        }
                    case "console":
                    case "c":
                        {
                            TelnetPerfmonServer program = new TelnetPerfmonServer();
                            program.Run(args.Skip(1).ToArray());
                            break;
                        }
                 /*   case "p":
                        {
                            TelnetPerfmonServer program = new TelnetPerfmonServer();
                            Thread t =new Thread(() => program.Run(new String[]{}));
                            t.Start();
                            Thread.Sleep(5000);
                            program.Stop();
                            break;
                        }*/
                }
            }
            else
            {
                bool found = false;
                foreach (ServiceController sc in ServiceController.GetServices())
                    if (sc.ServiceName == name)
                        found = true;
                if (!found)
                    install();
                else
                    ServiceBase.Run(new ServiceBase[] { new TelnetPerfmonService() });
            }
        }

        private static void install() {
            ManagedInstallerClass.InstallHelper(new string[] { Assembly.GetExecutingAssembly().Location });
            ServiceController[] scServices = ServiceController.GetServices();
            foreach (ServiceController sc in scServices)
                if (sc.ServiceName == name)
                    if (sc.Status != ServiceControllerStatus.Running && sc.Status != ServiceControllerStatus.StartPending)
                        sc.Start();
        }

        protected Thread m_thread;
        protected TelnetPerfmonServer program;
        override protected void OnStart(String[] args)
        {
            program = new TelnetPerfmonServer();
            ThreadStart ts = new ThreadStart(() => program.Run(args));
            
            m_thread = new Thread(ts);
            m_thread.Start();
            base.OnStart(args);
        }
        override protected void OnStop()
        {
           
            if (program != null)
                program.Stop();
            m_thread.Join(10000);
            base.OnStop();
        }
    }
}
