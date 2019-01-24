using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon;
using Amazon.CloudWatch.Model;
using Amazon.Lambda.Core;
using DotStep.Core;
using ModelBuilder.StateMachines;

namespace Tests
{
    class Program
    {
        static void Main(string[] args)
        {
            AWSConfigs.AWSRegion = "us-east-1";
            var program = new Program();
            //program.TestSessionProcessorAsync().Wait();

            program.TestCreateDashboard().Wait();
        }

        public async Task TestCreateDashboard()
        {
            var context = new ProvisionScene.Context
            {
                ClassNames = new List<string>
                {
                    "Purell", "Marker", "Eracer"
                    
                },
                SceneCode = "jfk14scene",
                CameraKey = "deeplens_PiaX2Z4dTQeJG7vG41vVtw",
                Region = "us-east-1"
            };
            var createDashboardTask = new ProvisionScene.CreateDashboard();

            await createDashboardTask.Execute(context);
        }


        public async Task TestSessionProcessorAsync()
        {
            var sessionProcessorFunction = new SessionProcessor.Function();

            await sessionProcessorFunction.FunctionHandler(new { }, new LambdaContext());
        }
    }

    public class ConsoleLogger : ILambdaLogger {
        public void Log(string message)
        {
            Console.Write(message);
        }

        public void LogLine(string message)
        {
            Console.WriteLine(message);
        }
    }


    public class LambdaContext : ILambdaContext {

        public LambdaContext()
        {
         Logger = new ConsoleLogger();   
        }

        public string AwsRequestId { get; }
        public IClientContext ClientContext { get; }
        public string FunctionName { get; }
        public string FunctionVersion { get; }
        public ICognitoIdentity Identity { get; }
        public string InvokedFunctionArn { get; }
        public ILambdaLogger Logger { get; }
        public string LogGroupName { get; }
        public string LogStreamName { get; }
        public int MemoryLimitInMB { get; }
        public TimeSpan RemainingTime { get; }
    }


}
