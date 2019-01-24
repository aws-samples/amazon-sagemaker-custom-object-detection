using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.CloudWatch.Model;

namespace SessionProcessor
{
    public class ObservationWindow
    {

        private readonly MetricDataResult metricData;

        public ObservationWindow(MetricDataResult metricData)
        {
            this.metricData = metricData;

            var values = new Dictionary<DateTime, double>();
            var i = metricData.Timestamps.Count - 1;
            foreach (var timestamp in metricData.Timestamps.OrderBy(t => t.Ticks))
            {
                var value = metricData.Values[i];
                values.Add(timestamp, value);
                i--;
            }
            Values = values;
        }

        public DateTime FirstObservation => metricData.Timestamps.Min();
        public DateTime LastObservation => metricData.Timestamps.Max();

        public string MetricName => metricData.Id;

        public Dictionary<DateTime, double> Values { get; }

    }
}