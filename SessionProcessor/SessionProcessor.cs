using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.Runtime.Internal;

namespace SessionProcessor
{
    public delegate void MetricsLoaded(int numberOfMetrics);

    public delegate void ObservationsCreated(int numberOfObservations, DateTime firstObservation,
        DateTime lastObservation);

    public delegate void SessionsDiscovered(int numberOfSessions);

    public class SessionProcessor
    {
        private readonly ISessionStore sessionStore;
        public List<Session> Sessions;

        public SessionProcessor(string cameraKey, List<string> classNames, string predictionEndpointName,
            double objectMovedDetectionThreshold = 0.25)
        {
            CameraKey = cameraKey;
            ClassNames = classNames;
            PredictionEndpointName = predictionEndpointName;
            ObjectMovedDetectionThreshold = objectMovedDetectionThreshold;
            sessionStore = new DynamoDBSessionStore();
        }

        public string CameraKey { get; }
        public List<string> ClassNames { get; }
        public string PredictionEndpointName { get; }

        public ObservationWindow PersonObservation { get; set; }
        public List<ObservationWindow> ClassObservations { get; set; }

        public List<MetricDataResult> MetricData { get; private set; }

        public double ObjectMovedDetectionThreshold { get; set; }
        public event MetricsLoaded MetricsLoaded;
        public event ObservationsCreated ObservationsCreated;
        public event SessionsDiscovered SessionsDiscovered;

        public async Task StoreSessions(bool storeCompletedSessionsWithNoItems = true)
        {
            var storageTasks = new List<Task>();
            foreach (var session in Sessions)
                switch (session.Status)
                {
                    case "COMPLETED":
                        if (session.Items.Count > 0)
                            storageTasks.Add(sessionStore.PutSession(session));
                        else if (storeCompletedSessionsWithNoItems)
                            storageTasks.Add(sessionStore.PutSession(session));
                        else storageTasks.Add(sessionStore.DeleteSession(session.Id));
                        break;
                    default:
                        storageTasks.Add(sessionStore.PutSession(session));
                        break;
                }
            await Task.WhenAll(storageTasks);
        }

        public async Task LoadMetrics(int minutes = 15, int period = 10)
        {
            var cloudWatch = new AmazonCloudWatchClient();

            var getMetricRequest = new GetMetricDataRequest
            {
                StartTimeUtc = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(minutes)),
                EndTimeUtc = DateTime.UtcNow,
                MetricDataQueries = new AutoConstructedList<MetricDataQuery>
                {
                    new MetricDataQuery
                    {
                        Id = "Person".ToLower(),
                        MetricStat = new MetricStat
                        {
                            Metric = new Metric
                            {
                                Namespace = "Cameras",
                                MetricName = "Confidence",
                                Dimensions = new AutoConstructedList<Dimension>
                                {
                                    new Dimension
                                    {
                                        Name = "CameraKey",
                                        Value = CameraKey
                                    },
                                    new Dimension
                                    {
                                        Name = "Label",
                                        Value = "Person"
                                    },
                                    new Dimension
                                    {
                                        Name = "Source",
                                        Value = "Rekognition"
                                    }
                                }
                            },
                            Period = period,
                            Stat = "Maximum",
                            Unit = StandardUnit.Percent
                        },
                        ReturnData = true
                    }
                }
            };

            foreach (var className in ClassNames)
                getMetricRequest.MetricDataQueries.Add(new MetricDataQuery
                {
                    Id = className.ToLower(),
                    MetricStat = new MetricStat
                    {
                        Metric = new Metric
                        {
                            Namespace = "Cameras",
                            MetricName = "Confidence",
                            Dimensions = new AutoConstructedList<Dimension>
                            {
                                new Dimension
                                {
                                    Name = "CameraKey",
                                    Value = CameraKey
                                },
                                new Dimension
                                {
                                    Name = "Label",
                                    Value = className
                                },
                                new Dimension
                                {
                                    Name = "Source",
                                    Value = PredictionEndpointName
                                }
                            }
                        },
                        Period = period,
                        Stat = "Maximum",
                        Unit = StandardUnit.Percent
                    },
                    ReturnData = true
                });

            var response = await cloudWatch.GetMetricDataAsync(getMetricRequest);
            MetricData = response.MetricDataResults;
            MetricsLoaded?.Invoke(MetricData.Count);
            CreateObservations();
        }

        private void CreateObservations()
        {
            if (MetricData.Count <= 1)
                return;

            if (!MetricData.All(md => md.Timestamps.Any()))
                return;

            var personMetrics = MetricData.First();
            var classMetrics = MetricData.Skip(1).ToList();
            PersonObservation = new ObservationWindow(personMetrics);
            ClassObservations = classMetrics.Select(classMetric => new ObservationWindow(classMetric)).ToList();

            ObservationsCreated?.Invoke(
                PersonObservation.Values.Count,
                PersonObservation.FirstObservation,
                PersonObservation.LastObservation);
        }

        private void DiscoverItems()
        {
            foreach (var session in Sessions.Where(s => s.Status == "COMPLETED"))
            foreach (var className in ClassNames)
            {
                var classObservations = ClassObservations.Single(co => co.MetricName == className.ToLower());

                var timeBeforeSession = classObservations.Values.Where(entry => entry.Key < session.Started)
                    .Max(entry => entry.Key);

                var timeAfterSession = classObservations.Values.Where(entry => entry.Key > session.Ended)
                    .Min(entry => entry.Key);

                var valueBeforeSession = classObservations.Values[timeBeforeSession];
                var valueAfterSession = classObservations.Values[timeAfterSession];

                var difference = valueBeforeSession - valueAfterSession;
                var threshold = valueBeforeSession * ObjectMovedDetectionThreshold;

                if (difference > threshold)
                    session.Items.Add(new Item
                    {
                        Name = className,
                        Price = 10
                    });
            }
        }

        public void DiscoverSessions()
        {
            Sessions = new List<Session>();

            var firstNoPersonFound = false;
            var personInPreviousEntry = false;
            var previousEntryTime = new DateTime();

            Session session = null;

            foreach (var personEntry in PersonObservation.Values)
            {
                // don't process a session that we don't have a start time for.
                if (!firstNoPersonFound && personEntry.Value < 20)
                    firstNoPersonFound = true;

                if (!firstNoPersonFound)
                    continue;

                var personInCurrentEntry = personEntry.Value > 20;

                if (personInCurrentEntry && !personInPreviousEntry)
                {
                    session = new Session(CameraKey, personEntry.Key);
                    Sessions.Add(session);
                }

                if (personInPreviousEntry && !personInCurrentEntry)
                    session.Ended = previousEntryTime;

                personInPreviousEntry = personInCurrentEntry;
                previousEntryTime = personEntry.Key;
            }

            DiscoverItems();
            SessionsDiscovered?.Invoke(Sessions.Count);
        }
    }
}