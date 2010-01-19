using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;

namespace PerformanceTools.Perfmon
{
    public interface SampleListener
    {
        void Data(String counterName, float value);
        void Data(String counterName, String instanceName, float value);
        void Category(String host, String name, bool hasInstances, DateTime time);
        void Close();
    }

    internal class LiveCategories : IEnumerable<String>
    {
        private string computerName;

        public LiveCategories(String computerName)
        {
            this.computerName = computerName;
        }

        public IEnumerator<string> GetEnumerator()
        {
            foreach (PerformanceCounterCategory category in PerformanceCounterCategory.GetCategories(computerName))
                yield return category.CategoryName;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            foreach (PerformanceCounterCategory category in PerformanceCounterCategory.GetCategories(computerName))
                yield return category.CategoryName;
        }
    }


    public class Perfmon
    {
        private IDictionary<String, InstanceDataCollectionCollection> previousSamples =
            new Dictionary<String, InstanceDataCollectionCollection>();

        private IDictionary<String, PerformanceCounterCategory> categories =
            new Dictionary<String, PerformanceCounterCategory>();


        private IDictionary<String, IList<SampleListener>> listenersPerCategory = new Dictionary<String, IList<SampleListener>>();


        private String computerName;


        public Perfmon(String computerName)
        {
            this.computerName = computerName;
        }

        public Perfmon() {
            this.computerName = Environment.MachineName;
        }

        public static IEnumerable<String> ListLiveCategoriesOn(String computerName)
        {
            return new LiveCategories(computerName);
        }

        public  IEnumerable<String> ListLiveCategories()
        {
            return new LiveCategories(computerName);
        }
        public List<String> Categories() {
            var result = new List<String>();
            foreach(String name in categories.Keys)
                result.Add(name);
            return result;
        }
        public PerformanceCounterCategory AddCategory(String categoryName, params SampleListener[] listeners)
        {
            if (categories.Keys.Contains(categoryName))
                throw new ArgumentException(categoryName + "is already registered");
            if (!categories.ContainsKey(categoryName))
            {
                PerformanceCounterCategory[] categoriesOnComputer =
                    PerformanceCounterCategory.GetCategories(computerName);
                if (categoriesOnComputer != null)
                {
                    foreach (PerformanceCounterCategory category in categoriesOnComputer)
                    {
                        if (category.CategoryName == categoryName)
                        {
                            categories.Add(category.CategoryName, category);
                       
                            listenersPerCategory.Add(category.CategoryName, listeners);
                            return category;
                        }
                    }
                }
            }
            return null;
        }


        public String Host() {
            return computerName;
        }
        public void Poll()
        {
            foreach (String categoryName in categories.Keys)
            {
                //Console.WriteLine(DateTime.Now +" polling " + categoryName);
                InstanceDataCollectionCollection previousCategoryData;
                previousSamples.TryGetValue(categoryName, out previousCategoryData);
                InstanceDataCollectionCollection currentCategoryData = categories[categoryName].ReadCategory();
                DateTime now = DateTime.Now;
                previousSamples[categoryName] = currentCategoryData;
                if (previousCategoryData != null)
                {
                    //                    Console.WriteLine(categories[categoryName].CategoryName);
                    foreach (SampleListener listener in listenersPerCategory[categoryName])
                        listener.Category(computerName,categories[categoryName].CategoryName,
                                      categories[categoryName].CategoryType ==
                                      PerformanceCounterCategoryType.MultiInstance, now);
                    IEnumerator counterKeys = currentCategoryData.Keys.GetEnumerator();
                    while (counterKeys.MoveNext())
                    {
                        InstanceDataCollection counter = currentCategoryData[(String)counterKeys.Current];
                        InstanceDataCollection previousCounter = previousCategoryData[(String)counterKeys.Current];
                        if (previousCounter != null)
                        {
                            if (categories[categoryName].CategoryType == PerformanceCounterCategoryType.MultiInstance)
                            {
                                IEnumerator instanceKeys = counter.Keys.GetEnumerator();
                                while (instanceKeys.MoveNext())
                                {
                                    InstanceData data = counter[(string)instanceKeys.Current];
                                    InstanceData previous = previousCounter[(string)instanceKeys.Current];
                                    if (previous != null)
                                    {
                                        float value =
                                           CounterSample.Calculate(previous.Sample, data.Sample);
                                        //Kanske skall vi använda CouterSample.Calculate instead
                                        //float value =
                                          //  CounterSampleCalculator.ComputeCounterValue(previous.Sample, data.Sample);
                                        foreach (SampleListener listener in listenersPerCategory[categoryName])
                                            listener.Data(counter.CounterName, data.InstanceName, value);
                                    }
                                }
                            }
                            else
                            {
                                IEnumerator instanceKeys = counter.Keys.GetEnumerator();
                                if (instanceKeys.MoveNext())
                                {
                                    CounterSample current = counter[(String)instanceKeys.Current].Sample;
                                    InstanceData previousInstance = previousCounter[(String)instanceKeys.Current];
                                    if (previousInstance != null)
                                    {
                                        //float value =
                                          //  CounterSampleCalculator.ComputeCounterValue(previousInstance.Sample,
                                            //                                            current);
                                        float value = CounterSample.Calculate(previousInstance.Sample, current);
                                        foreach (SampleListener listener in listenersPerCategory[categoryName])
                                            listener.Data(counter.CounterName, value);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public class PrintingSampleListener : SampleListener
    {
        private TextWriter writer;

        public PrintingSampleListener(TextWriter writer)
        {
            this.writer = writer;
        }

        public void Data(string counterName, float value)
        {
            lock(this)
            {
                writer.WriteLine("{0}, {1} ", counterName, value);
            }
        }

        public void Data(string counterName, string instanceName,
                         float value)
        {
            lock (this) {
                writer.WriteLine("{0}, {1}, {2} ", instanceName, counterName, value);
            }
        }

        public void Category(string host, string name, bool hasInstances, DateTime time)
        {
            lock (this) {
                writer.WriteLine(name + " " + time);
            }
        }

        public void Close() { }
    }

    public class SampleListenerFilter : SampleListener
    {
        private Regex instances;
        public  Regex Instances
        {
            get { lock (this)return instances; }
            set
            {
                lock (this) instances = value;
            }
        }
        private Regex counters;
        public Regex Counters
        {
            get { lock (this) return counters; }
            set { lock (this) counters = value; }
        }
        private Regex hosts;
        public Regex Hosts
        {
            get { lock (this) return hosts; }
            set { lock (this) hosts = value; }
        }
        private Regex categories;
        public Regex Categories
        {
            get { lock (this) return categories; }
            set { lock (this) categories = value; }
        }
        private readonly SampleListener listener;
        private bool isCurrentCategory= false;
        private bool isCurrentHost = false;
        private Boolean active;

        public void Start() {
            lock (this)
                active = true;
        }
        public void Stop()
        {
            lock(this)
                active = false;
        }
        public SampleListenerFilter(Regex counters, Regex instances, SampleListener listener)
        {
            this.Counters = counters;
            this.Instances = instances;
            this.listener = listener;
            this.Categories = new Regex(".*");
            this.Hosts = new Regex(".*");
            active = true;
        }
        public SampleListenerFilter(Regex categories, Regex counters, Regex instances, SampleListener listener)
        {
            this.Counters = counters;
            this.Instances = instances;
            this.listener = listener;
            this.Categories = categories;
            this.Hosts = new Regex(".*");
            active = true;
        }

        public SampleListenerFilter(Regex hosts, Regex categories, Regex counters, Regex instances, SampleListener listener)
        {
            this.Counters = counters;
            this.Instances = instances;
            this.listener = listener;
            this.Categories = categories;
            this.Hosts = hosts;
            active = true;
        }
        public SampleListenerFilter(Regex hosts, Regex categories, Regex counters, Regex instances, SampleListener listener, bool active)
        {
            this.Counters = counters;
            this.Instances = instances;
            this.listener = listener;
            this.Categories = categories;
            this.Hosts = hosts;
            this.active = active;
        }

        public void Data(string counterName, float value)
        {
            lock (this)
            {
                if (active && isCurrentCategory && isCurrentHost)
                {
                    lock (this)
                    {
                        if (Counters.IsMatch(counterName))
                            listener.Data(counterName, value);
                    }
                }
            }
        }

        public void Data(string counterName, string instanceName, float value)
        {
            lock (this)
            {
                if (active && isCurrentCategory && isCurrentHost)
                {
                    lock (this)
                    {
                        if (Counters.IsMatch(counterName) && Instances.IsMatch(instanceName))
                            listener.Data(counterName, instanceName, value);
                    }
                }
            }
        }

        public void Category(string host,string name, bool hasInstances, DateTime time)
        {
            lock (this)
            {
                if (active)
                {
                    isCurrentHost = Hosts.IsMatch(host);
                    isCurrentCategory = Categories.IsMatch(name);
                    if (isCurrentCategory && isCurrentHost)
                        listener.Category(host, name, hasInstances, time);
                }
            }
        }

        public void Close()
        {
            
            listener.Close();
        }
    }

    public class FileSampleListener : SampleListener
    {
        private IDictionary<String, StreamWriter> filesPerName = new Dictionary<String, StreamWriter>();
        private IDictionary<String, IList<String>> fieldsPerName = new Dictionary<String, IList<String>>();
        private IDictionary<String, List<String>> currentLinePerName = new Dictionary<String, List<String>>();
        //private IList<String> fixedNames = new List<String>();
        private IDictionary<String, IList<String>> namesPerCategory = new Dictionary<String, IList<String>>();

        private string currentCategory;
        private string prefix;

        public FileSampleListener(String path, String prefix)
        {
            DirectoryInfo dirInfo = new DirectoryInfo(path);
            if (dirInfo.Exists)
                dirInfo.Create();
            this.prefix = new FileInfo(dirInfo.FullName + "\\" + prefix).FullName;
        }

        public void Data(string counterName, float value)
        {
            lock (this) {
                AssureNamePerCategory(currentCategory, currentCategory);
                fill(currentCategory, counterName, value);
            }
        }

        private void AssureNamePerCategory(string category, string name)
        {
            if (!namesPerCategory.Keys.Contains(category))
            {
                namesPerCategory.Add(category, new List<String>());
            }
            IList<String> names = namesPerCategory[category];
            if (!names.Contains(name))
                names.Add(name);
        }

        public void Data(string counterName, string instanceName, float value)
        {
            lock (this) {
                String name = instanceName + "." + currentCategory;
                AssureNamePerCategory(currentCategory, name);
                fill(name, counterName, value);
            }
        }


        private void fill(String name, String counterName, float value)
        {

            if (!fieldsPerName.ContainsKey(name))
            {
                fieldsPerName.Add(name, new List<String>());
                currentLinePerName.Add(name, new List<String>());

            }
            if (filesPerName.Keys.Contains(name))
            {
                int index = fieldsPerName[name].IndexOf(counterName);
                if (index != -1)
                {
                    IList currentLine = currentLinePerName[name];
                    while (currentLine.Count < index + 1)
                        currentLine.Add("");
                    String va = value.ToString("0.############").Replace(",", ".");
                    //  Console.WriteLine(va);
                    currentLine[index] = va;
                }
            }
            else
            {

                String va = value.ToString("0.############").Replace(",", ".");
                //Console.WriteLine(va);
                currentLinePerName[name].Add(va);
                fieldsPerName[name].Add(counterName);
                if (currentLinePerName[name].Count != fieldsPerName[name].Count)
                    throw new ApplicationException();
            }

        }

        public void Category( string host, string name, bool hasInstances, DateTime time)
        {
            lock (this) {
                if (currentCategory != null)
                {
                    flush(currentCategory, time);
                }
                currentCategory = name;
            }
        }

        public void Close()
        {
            lock (this) {
                foreach (TextWriter writer in filesPerName.Values)
                    writer.Close();
            }
        }

        private String createGoodFileName(String name)
        {
            return name.Replace(' ', '_').Replace('\\', '-').Replace("/", "Per").Replace('$', '_').Replace(
                            ":", "_").Replace("?", "_");
        }

        private void flush(String category, DateTime time)
        {
            IList<String> names;
            if (namesPerCategory.TryGetValue(category, out names))
            {
                foreach (String name in currentLinePerName.Keys)
                {
                    if (names.Contains(name))
                    {
                        if (!filesPerName.ContainsKey(name))
                        {
                            filesPerName[name] = new StreamWriter(prefix + createGoodFileName("." + name + ".csv"), false);
                            filesPerName[name].Write("Date, Time");
                            foreach (String field in fieldsPerName[name])
                            {
                                filesPerName[name].Write(", " + field);
                            }
                            filesPerName[name].WriteLine();
                            // fixedNames.Add(name);
                        }
                        if (currentLinePerName[name].Count != 0)
                        {

                            filesPerName[name].Write(time.ToString("yyyy-MM-dd, HH':'mm':'ss"));
                            foreach (String field in currentLinePerName[name])
                            {
                                filesPerName[name].Write(", " + field);
                            }

                            filesPerName[name].WriteLine();
                            filesPerName[name].Flush();
                        }
                    }
                }
                foreach (String name in names)
                {
                    currentLinePerName[name] = new List<string>();
                }
            }
           
        }
    }
}
