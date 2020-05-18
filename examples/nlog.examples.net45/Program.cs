using NLog;
using NLog.Config;
using NLog.Kafka;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp2
{
    class Program
    {


        static void Main(string[] args)
        {
            //ConfigurationItemFactory.Default.Targets.RegisterDefinition("kafka", typeof(KafkaTarget));

            //LogManager.ReconfigExistingLoggers();
            Logger logger = LogManager.GetCurrentClassLogger();

            MappedDiagnosticsContext.Set("item1", "haha");
            string path = @"C:\Downloads\Elastic_Stack\text.log";
            long countString = 0;
            using (StreamReader sr = new StreamReader(path, System.Text.Encoding.Default))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    logger.Info(line);
                    countString++;
                }
            }
            Console.WriteLine("Sended " + countString + " messages to Kafka");
            //for (int i = 0; i < 10; i++)
            //{
            //    logger.Info("My test information");
            //    logger.Error(new NotImplementedException("error"),"error");
            //    Console.WriteLine("sended");
            //}
            Console.ReadKey();
        }
    }
}
