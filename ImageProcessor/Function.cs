using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Rekognition;
using Amazon.Rekognition.Model;
using Amazon.S3;
using Amazon.S3.Util;
using Amazon.SageMakerRuntime;
using Amazon.SageMakerRuntime.Model;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using Newtonsoft.Json;
using SixLabors.ImageSharp.Formats.Jpeg;
using SixLabors.ImageSharp.Processing;
using SixLabors.Primitives;
using Image = SixLabors.ImageSharp.Image;
using JsonSerializer = Amazon.Lambda.Serialization.Json.JsonSerializer;

[assembly: LambdaSerializer(typeof(JsonSerializer))]

namespace ImageProcessor
{
    public class Function
    {
        private readonly Dictionary<string, string> cameraParameters = new Dictionary<string, string>();
        private readonly IAmazonCloudWatch cloudWatch = new AmazonCloudWatchClient();
        private readonly IAmazonRekognition rekognition = new AmazonRekognitionClient();
        private readonly IAmazonS3 s3 = new AmazonS3Client();
        private readonly IAmazonSageMakerRuntime sageMakerRuntime = new AmazonSageMakerRuntimeClient();
        private readonly IAmazonSimpleSystemsManagement ssm = new AmazonSimpleSystemsManagementClient();

        public async Task FunctionHandler(S3Event s3Event, ILambdaContext context)
        {
            var tasks = new List<Task>();
            foreach (var record in s3Event.Records)
                tasks.Add(ProcessRecord(record, context));
            await Task.WhenAll(tasks);
        }

        public async Task ProcessRecord(S3EventNotification.S3EventNotificationRecord record, ILambdaContext context)
        {
            var cameraKey = record.S3.Object.Key.Split('/')[1];

            var s3GetResult = await s3.GetObjectAsync(record.S3.Bucket.Name, record.S3.Object.Key);

            var classNamesParameterName = $"/Cameras/{cameraKey}/ClassNames";
            var sceneCodeParameterName = $"/Cameras/{cameraKey}/SceneCode";
            var observationBoundingBoxParameterName = $"/Cameras/{cameraKey}/ObservationBoundingBox";

            if (!cameraParameters.ContainsKey(observationBoundingBoxParameterName))
                try
                {
                    var getResult = await ssm.GetParameterAsync(new GetParameterRequest
                    {
                        Name = observationBoundingBoxParameterName
                    });
                    cameraParameters.Add(observationBoundingBoxParameterName, getResult.Parameter.Value);
                    context.Logger.LogLine(
                        $"Set {observationBoundingBoxParameterName} = {cameraParameters[observationBoundingBoxParameterName]}");
                }
                catch (Exception exception)
                {
                    context.Logger.LogLine($"Didn't add parameter. {observationBoundingBoxParameterName}");
                    context.Logger.LogLine(exception.Message);
                }

            if (!cameraParameters.ContainsKey(classNamesParameterName))
            {
                var getResult = await ssm.GetParameterAsync(new GetParameterRequest
                {
                    Name = classNamesParameterName
                });
                cameraParameters.Add(classNamesParameterName, getResult.Parameter.Value);
                context.Logger.LogLine($"Set {classNamesParameterName} = {cameraParameters[classNamesParameterName]}");
            }

            if (!cameraParameters.ContainsKey(sceneCodeParameterName))
            {
                var getResult = await ssm.GetParameterAsync(new GetParameterRequest
                {
                    Name = sceneCodeParameterName
                });
                cameraParameters.Add(sceneCodeParameterName, getResult.Parameter.Value);
                context.Logger.LogLine($"Set {sceneCodeParameterName} = {cameraParameters[sceneCodeParameterName]}");
            }

            var memoryStream = new MemoryStream();
            await s3GetResult.ResponseStream.CopyToAsync(memoryStream);

            // Crop the area of interest.
            // TODO: Get the x, y, w, h from parmeter store.

            var croppedMemoryStream = new MemoryStream();
            var cropImage = cameraParameters.ContainsKey(observationBoundingBoxParameterName);
            if (cropImage)
            {
                var parts = cameraParameters[observationBoundingBoxParameterName].Split(',');
                memoryStream.Position = 0;
                var sourceImage = Image.Load(memoryStream);
                var x = int.Parse(parts[0]);
                var y = int.Parse(parts[1]);
                var lowerX = int.Parse(parts[2]);
                var lowerY = int.Parse(parts[3]);
                var w = lowerX - x;
                var h = lowerY - y;
                sourceImage.Mutate(i => i.Crop(new Rectangle(x, y, w, h)));

                context.Logger.LogLine("Trying to save croped image.");
                sourceImage.Save(croppedMemoryStream, new JpegEncoder());
                croppedMemoryStream.Position = 0;
            }


            var labelsResult = await rekognition.DetectLabelsAsync(new DetectLabelsRequest
            {
                Image = new Amazon.Rekognition.Model.Image
                {
                    Bytes = cropImage ? croppedMemoryStream : memoryStream
                },
                MaxLabels = 100,
                MinConfidence = 70
            });

            if (cropImage)
                croppedMemoryStream.Position = 0;
            else
                memoryStream.Position = 0;

            var metricData = new List<MetricDatum>();


            var personMetric = new MetricDatum
            {
                MetricName = "Confidence",
                StorageResolution = 1,
                TimestampUtc = DateTime.UtcNow,
                Unit = StandardUnit.Percent,
                Dimensions = new List<Dimension>
                {
                    new Dimension {Name = "CameraKey", Value = cameraKey},
                    new Dimension {Name = "Source", Value = "Rekognition"},
                    new Dimension {Name = "Label", Value = "Person"}
                }
            };

            if (labelsResult.Labels.Any(label => label.Name == "Person"))
            {
                var confidence = Convert.ToDouble(labelsResult.Labels.Single(l => l.Name == "Person").Confidence);
                personMetric.StatisticValues = new StatisticSet
                {
                    Minimum = confidence,
                    Maximum = confidence,
                    SampleCount = 1,
                    Sum = 1
                };
            }
            else
            {
                personMetric.StatisticValues = new StatisticSet
                {
                    Minimum = 0,
                    Maximum = 0,
                    SampleCount = 1,
                    Sum = 1
                };
            }

            metricData.Add(personMetric);


            var objectDetectionResult = await sageMakerRuntime.InvokeEndpointAsync(new InvokeEndpointRequest
            {
                Accept = "application/jsonlines",
                ContentType = "application/x-image",
                EndpointName = cameraParameters[sceneCodeParameterName],
                Body = cropImage ? croppedMemoryStream : memoryStream
            });

            if (cropImage)
                croppedMemoryStream.Close();
            else
                memoryStream.Close();

            using (var streamReader = new StreamReader(objectDetectionResult.Body))
            {
                var json = streamReader.ReadToEnd();

                context.Logger.Log($"SageMaker Endpoint Result: {json}");

                var predictionResult = JsonConvert.DeserializeObject<dynamic>(json).prediction;

                var classNames = cameraParameters[classNamesParameterName].Split(',');
                var predictions = new List<Prediction>();
                foreach (var pr in predictionResult)
                    predictions.Add(new Prediction
                    {
                        ClassName = classNames[Convert.ToInt32(pr[0].Value)],
                        Confidence = Convert.ToDouble(pr[1].Value) * 100
                    });

                foreach (var classNotPredicted in classNames.Where(cn => predictions.All(p => p.ClassName != cn)))
                    predictions.Add(new Prediction
                    {
                        ClassName = classNotPredicted,
                        Confidence = 0
                    });

                foreach (var classGroup in predictions.GroupBy(p => p.ClassName))
                    metricData.Add(new MetricDatum
                    {
                        MetricName = "Confidence",
                        StorageResolution = 1,
                        TimestampUtc = DateTime.UtcNow,
                        Unit = StandardUnit.Percent,
                        Dimensions = new List<Dimension>
                        {
                            new Dimension {Name = "CameraKey", Value = cameraKey},
                            new Dimension {Name = "Source", Value = cameraParameters[sceneCodeParameterName]},
                            new Dimension {Name = "Label", Value = classGroup.Key}
                        },
                        StatisticValues = new StatisticSet
                        {
                            Minimum = classGroup.Min(c => c.Confidence),
                            Maximum = classGroup.Max(c => c.Confidence),
                            Sum = classGroup.Count(),
                            SampleCount = classGroup.Count()
                        }
                    });
            }

            await cloudWatch.PutMetricDataAsync(new PutMetricDataRequest
            {
                Namespace = "Cameras",
                MetricData = metricData
            });
        }
    }

    public class Prediction
    {
        public string ClassName { get; set; }
        public double Confidence { get; set; }
    }
}