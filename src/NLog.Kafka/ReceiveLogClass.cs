using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NLog.Kafka
{
    public class ReceiveLogClass : LogClass
    {
        public string timestamp { get; set; }
        public string entityType { get; set; }
        public long entityId { get; set; }
        public string entityAction { get; set; }
    }
}
