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

namespace NLog.Kafka
{
    [Target("Kafka")]
    public class KafkaTarget : TargetWithLayout
    {

        private IProducer<Null, LogClass> _producer = null;

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


        protected override async void Write(LogEventInfo logEvent)
        {
            IPHostEntry ipHost = Dns.GetHostEntryAsync(Dns.GetHostName()).Result;

            IPAddress ipAddr = ipHost.AddressList[0];

            //IFormatter formatter = new BinaryFormatter();
            //LogClass log = (LogClass)formatter.Deserialize(logEvent.FormattedMessage); ;

            LogClass log = JsonConvert.DeserializeObject<LogClass>(logEvent.Message);

            Dictionary<string, object> htmlAttributes = new Dictionary<string, object>();

            if (!log.Message.Contains("Handled Mng"))
            {
                htmlAttributes = JsonConvert.DeserializeObject<Dictionary<string, object>>(log.Message);
            }

            Dictionary<string, object> formatLogEvent = new Dictionary<string, object>() {
                { "version"        , logEvent.SequenceID },
                { "@timestamp"     , DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture) },
                { "appname"        , this.appname },
                { "HOSTNAME"       , ipAddr.ToString() },
                { "thread_name"    , System.Threading.Thread.CurrentThread.Name },
                { "level"          , logEvent.Level.Name },
                { "logger_name"    , logEvent.LoggerName },
                { "action"         , log.Action},
                { "delivery_tag"   , log.DeliveryTag },
                { "exchange"       , log.Exchange},
                { "correlation_id" , log.CorrelationId },
                { "top_messages"   , log.Message },
                { "top_error"      , log.Error },
            };
            if (htmlAttributes != null)
            {
                foreach (KeyValuePair<string, object> entry in htmlAttributes)
                {
                    formatLogEvent.Add(entry.Key, entry.Value);
                }
            }

            if (logEvent.Exception != null)
            {
                formatLogEvent["message"] = logEvent.Exception.Message;
                formatLogEvent["stack_trace"] = logEvent.Exception.StackTrace;
            }


            if (includeMdc)
            {
                TransferContextDataToLogEventProperties(formatLogEvent);
            }
            //foreach (KeyValuePair<string, object> entry in formatLogEvent)
            //{ 
                Message<Null, LogClass> message = new Message<Null, LogClass> { Value = log};
                //LogClass object = JsonConvert.SerializeObject(formatLogEvent);
                await SendMessageToQueueAsync(message);
               //}

            base.Write(logEvent);
        }

        private static void TransferContextDataToLogEventProperties(Dictionary<string, object> logDic)
        {
            foreach (var contextItemName in MappedDiagnosticsContext.GetNames())
            {
                var key = contextItemName;

                if (!logDic.ContainsKey(key))
                {
                    var value = MappedDiagnosticsContext.Get(key);
                    logDic.Add(key, value);
                }
            }
        }

        #region kafka 
        private IProducer<Null, LogClass> GetProducer()
        {
            if (this.ProducerConfigs == null || this.ProducerConfigs.Count == 0) throw new Exception("ProducerConfigs is not found");

            if (_producer == null)
            {
               // IEnumerable<KeyValuePair<string, string>> config = new IEnumerable<KeyValuePair<string, string>>;
                var config = new Confluent.Kafka.ProducerConfig {};
                foreach (var pconfig in this.ProducerConfigs)
                {
                    config.Set(pconfig.Key, pconfig.value);
                }               
                _producer = new ProducerBuilder<Null, LogClass>(config).Build();
            }

            return _producer;
        }

        private async Task SendMessageToQueueAsync(Message<Null, LogClass> message)
        {

            /*if (message.Key==null)
                return;*/
            var producer = this.GetProducer();

            var key = "Multiple." + DateTime.Now.Ticks;

            //var dr =
                await producer.ProduceAsync(topic, message).ContinueWith(task => task.IsFaulted
                            ? $"error producing message: {task.Exception.Message}"
                            : $"produced to: {task.Result.TopicPartitionOffset}");
           //if (dr.Result != null)
           //{
           //    dr.ContinueWith(task =>
           //    {
           //        Console.WriteLine($"Delivered '{task.Result.Value}' to: {task.Result.Topic} message: {task.Result.Message.Value}");
           //    });
           // }

        }

        private void _producer_OnStatistics(object sender, string e)
        {
            Console.WriteLine($"nlog.kafka statistics: {e}");
        }

        private void _producer_OnLog(object sender, LogMessage e)
        {
            Console.WriteLine($"nlog.kafka on log: [ Level: {e.Level} Facility:{e.Facility} Name:{e.Name} Message:{e.Message} ]");
        }

        private void _producer_OnError(object sender, Error e)
        {
            Console.WriteLine($"nlog.kafka error: [ Code:{e.Code} HasError:{e.IsError} IsBrokerError:{e.IsBrokerError} IsLocalError:{e.IsLocalError} Reason:{e.Reason} ]");
        }

        private void CloseProducer()
        {
            if (_producer != null)
            {
                _producer?.Flush(TimeSpan.FromSeconds(60));
                _producer?.Dispose();
            }
            _producer = null;
        }

        #endregion


        protected override void CloseTarget()
        {
            CloseProducer();
            base.CloseTarget();
        }

    }
}
