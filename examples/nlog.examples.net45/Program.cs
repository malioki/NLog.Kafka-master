using Newtonsoft.Json;
using NLog;
using NLog.Targets;
using NLog.Config;
using NLog.Web;
using NLog.Kafka;
using System;
using System.IO;

namespace ConsoleApp2
{
    class Program
    {
        static void Main(string[] args)
        {
            /*Logger logger = LogManager.GetCurrentClassLogger();
            Logger nlog = LogManager.GetLogger("Kafka");*/
            Logger logger = LogManager.GetLogger("Messaging");
            Logger loggerFile = LogManager.GetLogger("AutoTest");
            string path = @"C:\Downloads\Elastic_Stack\text.txt";
            using (StreamReader sr = new StreamReader(path, System.Text.Encoding.Default))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    line = line.Substring(line.IndexOf('{'));
                    if (!line.Contains("Handled Mng"))
                    {
                        LogClass log = JsonConvert.DeserializeObject<LogClass>(line);
                        LogLevel level;
                        if (!log.Action.Contains("Error"))
                            level = LogLevel.Trace;
                        else
                            level = LogLevel.Error;

                        LogEventInfo logInfo = new LogEventInfo(level, log.Action, line);
                        if (level == LogLevel.Trace)
                            logger.Trace(logInfo);
                        else
                            loggerFile.Error(logInfo);
                    }
                }
            }
            Console.ReadKey();
        }
    }
}
