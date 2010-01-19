using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceTools.Perfmon;
using System.Text.RegularExpressions;
using System.Net.Sockets;
using System.Threading;
using System.Net;
using System.Text;
using IronPython;
using IronPython.Hosting;
using Microsoft.Scripting;
using Microsoft.Scripting.Hosting;
namespace PerfomanceTools.TelnetPerfmonServer
{
    class Program
    {
        static volatile bool isPolling = false;
        static void Main(string[] args)
        {
            try
            {

                ScriptEngine engine = Python.CreateEngine();
                
                ScriptScope scope = engine.CreateScope();
                scope.SetVariable("hosts", new Dictionary<String, Regex>());
                if (args.Count() < 3)
                {
                    Console.WriteLine("Supply:");
                    Console.WriteLine("port interval scriptfile");
                    Console.WriteLine("as arguments, where:");
                    Console.WriteLine("port is listener");
                    Console.WriteLine("interval is polling interval in seconds");
                    Console.WriteLine("scriptfile is a python file defining the configuration");
                    return;
                }
                int port = int.Parse(args[0]);
                int interval = int.Parse(args[1]);

                try
                {
                    engine.CreateScriptSourceFromFile(args[2]).Execute(scope);
                }
                catch (SyntaxErrorException e)
                {

                    Console.WriteLine(e.Message + " at " + e.SourcePath + ":" + e.Line + " \"" + e.SourceCode + "\"");
                    return;
                }
                Dictionary<String, Regex> confFromScript = scope.GetVariable<Dictionary<String, Regex>>("hosts");
                
                Dictionary<String, Regex> conf = new Dictionary<string, Regex>();
                foreach (var entry in confFromScript)
                try
                {
                    var ip=Dns.GetHostEntry(entry.Key);
                    if (conf.ContainsKey(ip.HostName)) {
                        Console.WriteLine("Host " + ip.HostName + " defined more than once");
                        return;
                    }

                    conf.Add(ip.HostName, entry.Value);

                }
                catch (SocketException)
                {
                    Console.WriteLine(entry.Key + " is not recognized as a host");
                    continue;
                }
                if (conf.Count == 0)
                {
                    Console.WriteLine("No hosts to monitor defined");
                    return;
                }
                SharedSampleListeners sharedlisteners = new SharedSampleListeners();
                Dictionary<String, Perfmon> perfmons = new Dictionary<String, Perfmon>();
               
                foreach (var host in conf)
                {
                    perfmons.Add(host.Key, new Perfmon(host.Key));
                }


                foreach (var host in conf)
                {
                    
                  
                    foreach (var category in perfmons[host.Key].ListLiveCategories())
                        if (host.Value.IsMatch(category))
                        {
                            perfmons[host.Key].AddCategory(category, sharedlisteners);
                            Console.WriteLine(host.Key + ": " + category);
                        }
                }
                List<String> perfmonsToRemove = new List<String>();
                foreach (var perfmon in perfmons)
                {
                    if (perfmon.Value.Categories().Count == 0)
                    {
                        perfmonsToRemove.Add(perfmon.Key);
                        Console.WriteLine(perfmon.Key + " has no suitable counters to monitor");
                    }
                }
                foreach (var toRemove in perfmonsToRemove)
                    perfmons.Remove(toRemove);

                if (perfmons.Count == 0)
                {
                    Console.WriteLine("Nothing to monitor");
                    return;
                }

                Console.WriteLine("Checking every " + interval + " s");
                Console.WriteLine("Data available on port " + port);
                Console.WriteLine("... ");
                Object lockObject = new Object();
                Timer timer = new Timer((object o) =>
                {
                    if (!isPolling)
                    {
                        lock (lockObject)
                        {
                            isPolling = true;
                            try
                            {
                                foreach (Perfmon perfmon in perfmons.Values)
                                    try
                                    {
                                        perfmon.Poll();
                                    }
                                    catch (Exception e)
                                    {
                                        Console.WriteLine(perfmon.Host() + " could not be polled." + e.Message);
                                    }
                            }
                            finally { isPolling = false; }
                        }
                    }
                    else
                    {
                        Console.WriteLine("Exhausted at " + new DateTime());
                    }
                }, null, 0, interval * 1000);
                try
                {


                    TcpListener listener = new TcpListener(new IPEndPoint(IPAddress.Any, port));
                    listener.Start();
                    Console.WriteLine("Listening");

                    while (true)
                    {

                        TcpClient client = listener.AcceptTcpClient();
                        client.SendTimeout = 5000;

                        NetworkStream stream = client.GetStream();
                        TextWriter sw = TextWriter.Synchronized(new StreamWriter(stream) { AutoFlush = false });
                        StreamReader sr = new StreamReader(stream);
                        try
                        {
                            SampleListenerFilter sl = new SampleListenerFilter(new Regex(".*"), new Regex(".*"), new Regex(".*"), new Regex(".*"), new StructSampleListener(sw), false);

                            sharedlisteners.Add(sl);
                            Thread t = new Thread(new CommandLoop(sw, sr, sl).commandLoop);
                            t.IsBackground = true;
                            t.Start();
                            //Appearently, we dont need to cleanup the handle in .NET!?!?
                        }
                        catch (Exception)
                        {
                            sw.WriteLine("Sorry... bye bye");
                            stream.Close();
                        }
                    }
                }
                finally
                {
                    timer.Dispose();
                }



            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    class CommandLoop
    {
        private readonly TextWriter output;
        private readonly TextReader input;
        private readonly SampleListenerFilter filter;
        private static Regex hostsPattern = new Regex("^hosts (.*)");
        private static Regex categoriesPattern = new Regex("^categories (.*)");
        private static Regex countersPattern = new Regex("^counters (.*)");
        private static Regex instancesPattern = new Regex("^instances (.*)");
        public CommandLoop(TextWriter output, TextReader input, SampleListenerFilter filter)
        {
            this.input = input;
            this.output = output;
            this.filter = filter;
        }
        public void commandLoop()
        {
            try
            {
                String line = null;
                while (null != (line = input.ReadLine()))
                {
                    try
                    {
                        Match m = hostsPattern.Match(line);
                        if (m.Success)
                        {
                            String pattern = m.Groups[1].Value;
                            Regex r = new Regex(pattern);
                            output.WriteLine("I Hosts pattern:" + pattern);
                            filter.Hosts = r;
                            continue;
                        }
                        m = categoriesPattern.Match(line);
                        if (m.Success)
                        {
                            String pattern = m.Groups[1].Value;
                            Regex r = new Regex(pattern);
                            output.WriteLine("I Categories pattern:" + pattern);
                            filter.Categories = r;
                            continue;
                        }
                        m = countersPattern.Match(line);
                        if (m.Success)
                        {
                            String pattern = m.Groups[1].Value;
                            Regex r = new Regex(pattern);
                            output.WriteLine("I Counters pattern:" + pattern);
                            filter.Counters = r;
                            continue;
                        }
                        m = instancesPattern.Match(line);
                        if (m.Success)
                        {
                            String pattern = m.Groups[1].Value;
                            Regex r = new Regex(pattern);
                            output.WriteLine("I Instances pattern:" + pattern);
                            filter.Instances = r;
                            continue;
                        }

                    }
                    catch (ArgumentException)
                    {
                        output.WriteLine("I Illegal pattern");
                    }
                    if (line.ToLower().Equals("start"))
                    {
                        filter.Start();
                    }
                    else if (line.ToLower().Equals("stop"))
                    {
                        filter.Stop();
                    }
                    else if (line.ToLower().Equals("quit"))
                    {
                        break;
                    }
                    else
                    {
                        output.WriteLine("I Huh?");
                        output.WriteLine("I Use commands: start, stop, hosts regex, categories regex, counters regex, instances regex, or quit");
                    }
                }
            }
            catch (IOException) { }
            catch (SocketException) { }
            finally
            {
                try
                {
                    output.Close();
                    input.Close();
                }
                catch (IOException) { }
                catch (SocketException) { }
            }
        }
    }

    class SharedSampleListeners : SampleListener {
        private readonly List<SampleListener> sampleListeners = new List<SampleListener>();
        private readonly List<SampleListener> newSampleListeners = new List<SampleListener>();
        private volatile Boolean closed = false;
        
        public void Add(SampleListener listener) {
            if (!closed)
            {
                lock (newSampleListeners)
                {
                    newSampleListeners.Add(listener);
                }
            }
        }



        #region SampleListener Members

        public void Data(string counterName, float value)
        {
            if (!closed)
            {
                List<SampleListener> toRemove = new List<SampleListener>();

                foreach (SampleListener listener in sampleListeners)
                {
                    try
                    {
                        listener.Data(counterName, value);
                    }
                    catch (Exception)
                    {
                        toRemove.Add(listener);
                    }
                }
                foreach (SampleListener listener in toRemove)
                {
                    sampleListeners.Remove(listener);
                    listener.Close();
                }
            }
        }

        public void Data(string counterName, string instanceName, float value)
        {
            if (!closed)
            {
                List<SampleListener> toRemove = new List<SampleListener>();
                foreach (SampleListener listener in sampleListeners)
                {
                    try
                    {
                        listener.Data(counterName, instanceName, value);
                    }
                    catch (Exception)
                    {
                        toRemove.Add(listener);
                    }
                }
                foreach (SampleListener listener in toRemove)
                {
                    sampleListeners.Remove(listener);
                    listener.Close();
                }
            }
        }

        public void Category(string host, string name, bool hasInstances, DateTime time)
        {
            if (!closed)
            {
                lock (newSampleListeners)
                {
                    foreach (SampleListener listener in newSampleListeners)
                        sampleListeners.Add(listener);
                    newSampleListeners.Clear();
                }

                List<SampleListener> toRemove = new List<SampleListener>();
                foreach (SampleListener listener in sampleListeners)
                {
                    try
                    {
                        listener.Category(host, name, hasInstances, time);
                    }
                    catch (Exception)
                    {
                        toRemove.Add(listener);
                    }
                }
                foreach (SampleListener listener in toRemove)
                {
                    sampleListeners.Remove(listener);
                    listener.Close();
                }
            }
        }

        public void Close()
        {
            closed = true;
            lock (newSampleListeners)
            {
                foreach (SampleListener listener in sampleListeners)
                    listener.Close();
            }
        }

        #endregion
    }
    class StructSampleListener : SampleListener {
        
        private TextWriter writer;
        //Category numbers
        private int nextCategoryNumber = 0;
        private Dictionary<String, int> categoryNumbers = new Dictionary<String, int>();
        private int nextHostNumber = 0;
        private Dictionary<String, int> hostNumbers = new Dictionary<String, int>();

        //The current categories
        private int currentCategory = -1;
        private int currentHost = -1;
        private Dictionary<String, int> currentCategoryWithoutInstance;
        private Dictionary<string, Dictionary<string, int>> currentCategoryWithInstance;
        private Dictionary<string, int> currentCounterIdForCategoriesWithInstance;
        //All categories
        private Dictionary<String, Dictionary<String, int>> keyWithoutInstance = new Dictionary<string, Dictionary<string, int>>();
        private Dictionary<string, Dictionary<string, Dictionary<string, int>>> keyWithInstance = new Dictionary<string, Dictionary<string, Dictionary<string, int>>>();
        private Dictionary<String, Dictionary<String, int>> counterIdsForCategoriesWithInstances = new Dictionary<string, Dictionary<string, int>>();
        

        public StructSampleListener(TextWriter writer)
        {
            this.writer = writer;
        }

        public void Data(String counterName, float value) {
            int counterId=-1;
            if (!currentCategoryWithoutInstance.ContainsKey(counterName))
            {
                if (currentCategoryWithoutInstance.Count == 0)
                    counterId = 0;
                else{
                   counterId=currentCategoryWithoutInstance.Values.Max()+1;
                }
                   currentCategoryWithoutInstance.Add(counterName, counterId);
                   writer.WriteLine("C " + currentHost + "." + currentCategory + "." + counterId + " " + counterName);
            }
            else
            {
                counterId = currentCategoryWithoutInstance[counterName];
            }
            String valueString = value.ToString("0.############").Replace(",", ".");
            writer.WriteLine("D " + currentHost+"."+currentCategory+"."+counterId+" " + valueString);
        }

        public void Data(String counterName, String instanceName, float value) {
            //resolve the counter
            int counterId = -1;
            Dictionary<String, int> instances=null;
            if (!currentCounterIdForCategoriesWithInstance.ContainsKey(counterName))
            {
                if (currentCounterIdForCategoriesWithInstance.Count == 0)
                    counterId = 0;
                else
                    counterId = currentCounterIdForCategoriesWithInstance.Values.Max() + 1;
                currentCounterIdForCategoriesWithInstance.Add(counterName, counterId);
                instances = new Dictionary<String, int>();
                currentCategoryWithInstance.Add(counterName, instances);
                writer.WriteLine("C " + currentHost + "." + currentCategory + "." + counterId +  " " + counterName);
            }
            else 
            {
                counterId = currentCounterIdForCategoriesWithInstance[counterName];
                instances = currentCategoryWithInstance[counterName];
            }
            //the instance
            int instanceId = -1;
            if(!instances.ContainsKey(instanceName)){
                if (instances.Count == 0)
                    instanceId = 0;
                else
                    instanceId=instances.Values.Max()+1;
                instances.Add(instanceName, instanceId);
                writer.WriteLine("C " + currentHost + "." + currentCategory + "." + counterId + "." + instanceId + " " + instanceName);
            }else{
                instanceId=instances[instanceName];
            }
            String valueString = value.ToString("0.############").Replace(",", ".");
            writer.WriteLine("D " + currentHost + "." + currentCategory + "." + counterId + "."+instanceId+" " + valueString);
            
        }

        public void Category(String host,String name, bool hasInstances, DateTime time)
        {
            if (hostNumbers.ContainsKey(host))
            {
                currentHost = hostNumbers[host];
            }
            else
            {
                currentHost = nextHostNumber++;
                hostNumbers.Add(host, currentHost);
                writer.WriteLine("C "+currentHost+" "+host);
            }
            if (hasInstances)
            {
                if (keyWithInstance.ContainsKey(name))
                {
                    currentCategoryWithInstance = keyWithInstance[name];
                    currentCounterIdForCategoriesWithInstance = counterIdsForCategoriesWithInstances[name];
                }
                else
                {
                    currentCategoryWithInstance = new Dictionary<string, Dictionary<string, int>>();
                    categoryNumbers.Add(name, nextCategoryNumber++);
                    keyWithInstance.Add(name, currentCategoryWithInstance);

                    currentCounterIdForCategoriesWithInstance = new Dictionary<string, int>();
                    counterIdsForCategoriesWithInstances.Add(name,currentCounterIdForCategoriesWithInstance);
                    writer.WriteLine("C "+currentHost+"."+categoryNumbers[name] + " "+name);
                }
            }
            else {
                if (keyWithoutInstance.ContainsKey(name))
                {
                    currentCategoryWithoutInstance = keyWithoutInstance[name];
                }
                else
                {
                    currentCategoryWithoutInstance = new Dictionary<string, int>();
                    categoryNumbers.Add(name, nextCategoryNumber++);
                    keyWithoutInstance.Add(name, currentCategoryWithoutInstance);
                    writer.WriteLine("C "+currentHost+"."+categoryNumbers[name] + " "+name);
                }
            }
            
            String timeString = time.ToString("yyyyMMdd HHmmss");
            writer.WriteLine("T " + timeString);
            currentCategory = categoryNumbers[name];
        }
        public void Close()
        {
            writer.Close();
        }
    }
}
