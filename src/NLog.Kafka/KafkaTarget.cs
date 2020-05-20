using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.Kafka.Admin;
using Newtonsoft.Json;
using NLog.Common;
using NLog.Config;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using NLog.Fluent;
using Newtonsoft.Json.Linq;

namespace NLog.Kafka
{
    [Target("Kafka")]
    public class KafkaTarget : TargetWithLayout
    {

        public KafkaTarget()
        {
            this.ProducerConfigs = new List<ProducerConfig>(10);
        }

        [RequiredParameter]
        [ArrayParameter(typeof(ProducerConfig), "producerConfig")]
        public IList<ProducerConfig> ProducerConfigs { get; set; }

        [RequiredParameter]
        public string appname { get; set; }

        [RequiredParameter]
        public string topic { get; set; }

        [RequiredParameter]
        public bool includeMdc { get; set; }

        protected override void Write(LogEventInfo logEvent)
        {
            var config = new Confluent.Kafka.ProducerConfig{};
            LogClass log = JsonConvert.DeserializeObject<LogClass>(logEvent.Message);
            if (!log.Message.Contains("Handled Mng"))
            {
                foreach (var pconfig in this.ProducerConfigs)
                {
                    config.Set(pconfig.Key, pconfig.value);
                }
                using (var producer = new ProducerBuilder<string, string>(config).Build())
                {
                    int numProduced = 0;
                    int numMessages = 1;
                    for (int i = 0; i < numMessages; ++i)
                    {
                        var key = "kafka";
                        var val = JObject.FromObject(log).ToString(Formatting.None);

                        Console.WriteLine($"Producing record: {key} {val}");

                        producer.Produce(topic, new Message<string, string> { Key = key, Value = val },
                            (deliveryReport) =>
                            {
                                if (deliveryReport.Error.Code != ErrorCode.NoError)
                                {
                                    Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                                }
                                else
                                {
                                    Console.WriteLine($"Produced message to: {deliveryReport.TopicPartitionOffset}");
                                    numProduced += 1;
                                }
                            });
                    }

                    producer.Flush(TimeSpan.FromSeconds(10));

                    Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
                }
            }
        }

    }
}
