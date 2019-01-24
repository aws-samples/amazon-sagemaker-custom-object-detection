using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.Serialization.Json;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(JsonSerializer))]

namespace SessionProcessor
{
    public class Function
    {
        private readonly IAmazonSimpleSystemsManagement ssm = new AmazonSimpleSystemsManagementClient();

        public async Task FunctionHandler(dynamic @event, ILambdaContext context)
        {
            var parameters = new List<Parameter>();
            string nextToken = null;

            Query:
            var parametersResult = await ssm.GetParametersByPathAsync(new GetParametersByPathRequest
            {
                Path = "/Cameras/",
                Recursive = true,
                NextToken = nextToken
            });
            parameters.AddRange(parametersResult.Parameters);
            nextToken = parametersResult.NextToken;
            if (!string.IsNullOrEmpty(nextToken))
                goto Query;

            var cameraKeys = parameters.Select(p => p.Name.Split('/')[2]).Distinct();

            foreach (var cameraKey in cameraKeys)
            {
                context.Logger.LogLine($"Processing cameraKey: {cameraKey}");

                var classNames = parameters.Single(p => p.Name == $"/Cameras/{cameraKey}/ClassNames")
                    .Value.Split(',').ToList();

                var sceneCode = parameters.Single(p => p.Name == $"/Cameras/{cameraKey}/SceneCode")
                    .Value;

                var objectMovedDetectionThreshold = 0.25;

                if (parameters.Any(
                    p => p.Name == $"/Cameras/{cameraKey}/ObjectMovedDetectionThreshold"))
                    objectMovedDetectionThreshold = Convert.ToDouble(
                        parameters.Single(p => p.Name == $"/Cameras/{cameraKey}/ObjectMovedDetectionThreshold").Value);

                var enabled = Convert.ToBoolean(parameters
                    .Single(p => p.Name == $"/Cameras/{cameraKey}/Enabled")
                    .Value);

                if (!enabled) continue;

                context.Logger.LogLine($"Processing camera: {cameraKey}");

                var sp = new SessionProcessor(cameraKey, classNames, sceneCode, objectMovedDetectionThreshold);

                sp.MetricsLoaded += metrics =>
                {
                    context.Logger.LogLine($"{metrics} metrics loaded. Camera: {cameraKey}");
                };

                sp.ObservationsCreated += (observations, earliest, latest) =>
                {
                    context.Logger.LogLine(
                        $"Metrics transformed, number of observations: {observations}, earliest: {earliest}, latest: {latest}. Camera: {cameraKey}");
                    sp.DiscoverSessions();
                };

                sp.SessionsDiscovered += sessions =>
                {
                    context.Logger.LogLine($"{sessions} sessions discovered. Camera: {cameraKey}");
                    sp.StoreSessions(false).Wait();
                };

                // this should run 5 checks per minute (every 10 seconds).
                for (var i = 0; i < 5; i++)
                    await Task.WhenAll(sp.LoadMetrics(5), Task.Delay(TimeSpan.FromSeconds(10)));
            }
        }
    }
}