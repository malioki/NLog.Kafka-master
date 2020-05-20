using NLog;
using NLog.Config;
using NLog.Kafka;
using System;
using System.IO;

namespace ConsoleApp2
{
    class Program
    {
        static void Main(string[] args)
        {
            Logger logger = LogManager.GetCurrentClassLogger();
            string path = @"C:\Downloads\Elastic_Stack\text.txt";
            using (StreamReader sr = new StreamReader(path, System.Text.Encoding.Default))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    line = line.Substring(line.IndexOf('{'));
                    logger.Info(line);
                }
            }
            Console.ReadKey();
        }
    }
}
