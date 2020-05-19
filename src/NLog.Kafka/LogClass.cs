using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NLog.Kafka
{
    public class LogClass
    {
        public string Action { get; set; }
        public int DeliveryTag { get; set; }
        public string Exchange { get; set; } 
        public string CorrelationId { get; set; }
        public string Message { get; set; }
        public string Error { get; set; }
    }
}
