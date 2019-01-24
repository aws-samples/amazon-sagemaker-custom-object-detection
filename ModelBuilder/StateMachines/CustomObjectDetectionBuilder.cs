using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.Runtime.Internal;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SageMaker;
using Amazon.SageMaker.Model;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotStep.Core;
using Newtonsoft.Json;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Formats.Jpeg;
using SixLabors.ImageSharp.Formats.Png;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;
using SixLabors.Primitives;
using Tag = Amazon.SageMaker.Model.Tag;

namespace ModelBuilder.StateMachines
{
    public static class MethodExtensions
    {
        public static string S3Bucket(this string s3Location) => s3Location.Split('/')[2];

        public static string S3Key(this string s3Location) =>
            s3Location.Replace($"s3://{s3Location.S3Bucket()}/", string.Empty);
    }


    public sealed class ProvisionScene : StateMachine<ProvisionScene.Initialize>
    {
        public class Context : IContext
        {
            [DotStep.Core.Required]
            public List<string> ClassNames { get; set; }
            [DotStep.Core.Required]
            public string SceneImageLocation { get; set; }
            [DotStep.Core.Required]
            public string Region { get; set; }
            [DotStep.Core.Required]
            public string CameraKey { get; set; }
            public string CameraBucket { get; set; }

            public string SceneBackgroundLocation { get; set; }
            public bool GenerateSceneBackground { get; set; }
           
            public string WorkteamArn { get; set; }
            public string LabelingRoleArn { get; set; }

            public string SceneProvisioningJobId { get; set; }
            public string SceneProvisioningJobWorkspace { get; set; }
            public string InputManifestLocation { get; set; }
            public string UiTemplateLocation { get; set; }

            public int LabelingJobPercentComplete { get; set; }


            public string SceneBackgroundGenerationQueueUrl { get; set; }
            public int BackgroundImagePercentComplete { get; set; }

            public int ImagesGeneratedPerClass { get; set; }

            public int NumberOfTrainingSamples { get; set; }

            public string SceneCode { get; set; }
            public int TrainingJobPercentComplete { get; set; }
            public int EndpointPercentComplete { get; set; }

            public int MotionThreshold { get; set; }
        }



        [DotStep.Core.Action(ActionName = "*")]
        public sealed class Initialize : TaskState<Context, CreateSegmentationLabelingJob>
        {
            IAmazonSimpleSystemsManagement ssm = new AmazonSimpleSystemsManagementClient();
            IAmazonS3 s3 = new AmazonS3Client();

            public override async Task<Context> Execute(Context context)
            {
                context.SceneProvisioningJobId = $"spj-{Guid.NewGuid().ToString().Substring("e9857953-2fc2-477e-b347-".Length, "6053c7f7b710".Length).ToLower()}";
                
                if (context.ClassNames.Count < 1)
                    throw new Exception("ClassNames are required.");

                var parameters = await ssm.GetParametersAsync(new GetParametersRequest
                {
                    Names = new List<string>
                    {
                        "/scene-provision/WorkteamArn",
                        "/scene-provision/LabelingRoleArn",
                        "/scene-provision/JobWorkspace",
                        "/scene-provision/SceneBackgroundGenerationQueueUrl"
                    }
                });

               
                context.WorkteamArn = parameters.Parameters.Single(p => p.Name == "/scene-provision/WorkteamArn").Value;
                context.LabelingRoleArn = parameters.Parameters.Single(p => p.Name == "/scene-provision/LabelingRoleArn").Value;
                context.SceneProvisioningJobWorkspace =
                    $"{parameters.Parameters.Single(p => p.Name == "/scene-provision/JobWorkspace").Value}{context.SceneProvisioningJobId}/";
                
                context.GenerateSceneBackground = string.IsNullOrEmpty(context.SceneBackgroundLocation);

                if (context.GenerateSceneBackground)
                    context.SceneBackgroundGenerationQueueUrl = parameters.Parameters
                        .Single(p => p.Name == "/scene-provision/SceneBackgroundGenerationQueueUrl").Value;
                else
                {
                    await s3.CopyObjectAsync(context.SceneBackgroundLocation.S3Bucket(),
                        context.SceneBackgroundLocation.S3Key(),
                        context.SceneProvisioningJobWorkspace.S3Bucket(),
                        context.SceneProvisioningJobWorkspace.S3Key() + "scene-background.jpg");
                    context.BackgroundImagePercentComplete = 100;
                    context.SceneBackgroundLocation = context.SceneProvisioningJobWorkspace + "scene-background.jpg";
                }

                if (context.MotionThreshold <= 0)
                    context.MotionThreshold = 200;

                var inputManifestBody = $"{{\"source-ref\": \"{context.SceneProvisioningJobWorkspace}input-scene.jpg\"}}";
                context.InputManifestLocation = $"{context.SceneProvisioningJobWorkspace}input-manifest.json";

                context.UiTemplateLocation = $"{context.SceneProvisioningJobWorkspace}Segmentation.xhtml";
                var uiTemplateBody = File.OpenText("Segmentation.xhtml").ReadToEnd();

                if (string.IsNullOrEmpty(context.SceneCode))
                {
                    var locationParts = context.SceneImageLocation.Split('/');
                    context.SceneCode = locationParts[locationParts.Length - 1];
                    context.SceneCode = context.SceneCode.Split('.')[0];
                    context.SceneCode = context.SceneCode.Replace("-", string.Empty).Replace("_", string.Empty).ToLower();
                }

                if (string.IsNullOrEmpty(context.CameraBucket))
                    context.CameraBucket = context.SceneProvisioningJobWorkspace.S3Bucket();

                await Task.WhenAll(new List<Task>
                {
                    ssm.PutParameterAsync(new PutParameterRequest
                    {
                        Name = $"/Cameras/{context.CameraKey}/ClassNames",
                        Value = string.Join(',', context.ClassNames),
                        Type = Amazon.SimpleSystemsManagement.ParameterType.String,
                        Overwrite = true
                    }),
                    ssm.PutParameterAsync(new PutParameterRequest
                    {
                        Name = $"/Cameras/{context.CameraKey}/SceneCode",
                        Value = context.SceneCode,
                        Type = Amazon.SimpleSystemsManagement.ParameterType.String,
                        Overwrite = true
                    }),
                    ssm.PutParameterAsync(new PutParameterRequest
                    {
                        Name = $"/Cameras/{context.CameraKey}/CameraBucket",
                        Value = context.CameraBucket,
                        Type = Amazon.SimpleSystemsManagement.ParameterType.String,
                        Overwrite = true
                    }),
                    ssm.PutParameterAsync(new PutParameterRequest
                    {
                        Name = $"/Cameras/{context.CameraKey}/Enabled",
                        Value = "False",
                        Type = Amazon.SimpleSystemsManagement.ParameterType.String,
                        Overwrite = true
                    }),
                    ssm.PutParameterAsync(new PutParameterRequest
                    {
                        Name = $"/Cameras/{context.CameraKey}/MotionThreshold",
                        Value = Convert.ToString(context.MotionThreshold),
                        Type = Amazon.SimpleSystemsManagement.ParameterType.String,
                        Overwrite = true
                    }),
                    s3.PutObjectAsync(new PutObjectRequest
                    {
                        BucketName = context.UiTemplateLocation.S3Bucket(),
                        Key = context.UiTemplateLocation.S3Key(),
                        ContentType = "application/xhtml+xml",
                        ContentBody = uiTemplateBody
                    }),
                    s3.PutObjectAsync(new PutObjectRequest
                    {
                        BucketName = context.InputManifestLocation.S3Bucket(),
                        Key = context.InputManifestLocation.S3Key(),
                        ContentType = "application/json",
                        ContentBody = inputManifestBody
                    }),
                    s3.PutObjectAsync(new PutObjectRequest
                    {
                        BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                        Key = context.SceneProvisioningJobWorkspace.S3Key() + "classes.csv",
                        ContentType = "text/csv",
                        ContentBody = string.Join(',', context.ClassNames)
                    }),
                    s3.CopyObjectAsync(new CopyObjectRequest
                    {
                        SourceBucket = context.SceneImageLocation.S3Bucket(),
                        SourceKey = context.SceneImageLocation.S3Key(),
                        DestinationBucket = context.SceneProvisioningJobWorkspace.S3Bucket(),
                        DestinationKey = $"{context.SceneProvisioningJobWorkspace.S3Key()}input-scene.jpg",
                        ContentType = "image/jpg"
                    })
                });

                if (context.ImagesGeneratedPerClass < 1)
                    context.ImagesGeneratedPerClass = 10;


                    
                return context;
            }
        }


        public sealed class CheckIfJobComplete : ChoiceState<ExtractPolygons>
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<WaitBeforeCheckingLabelingJobStatus, Context>(c => c.LabelingJobPercentComplete < 100)
            };
        }

        public sealed class WaitBeforeCheckingLabelingJobStatus : WaitState<GetLabelingJobStatus>
        {
            public override int Seconds => 60;
        }

        [DotStep.Core.Action(ActionName = "*")]
        public sealed class GetLabelingJobStatus : TaskState<Context, CheckIfJobComplete>
        {
            readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();
            public override async Task<Context> Execute(Context context)
            {

                var result = await sageMaker.DescribeLabelingJobAsync(
                    new DescribeLabelingJobRequest
                    {
                        LabelingJobName = context.SceneProvisioningJobId
                    });

                switch (result.LabelingJobStatus)
                {
                    case "InProgress":
                        context.LabelingJobPercentComplete = 50;
                        break;
                    case "Completed":
                        context.LabelingJobPercentComplete = 100;
                        break;
                    default: throw new Exception($"Labeling job status = {result.LabelingJobStatus}.");
                }
                Console.WriteLine($"Percent complete: {context.LabelingJobPercentComplete}");

                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "*")]
        public sealed class CreateSegmentationLabelingJob : TaskState<Context, CheckIfJobComplete>
        {
            readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();
            readonly IAmazonS3 s3 = new AmazonS3Client();

            public override async Task<Context> Execute(Context context)
            {

                Console.Write(JsonConvert.SerializeObject(context));

                //var resp = await sageMaker.DescribeLabelingJobAsync(new DescribeLabelingJobRequest
               // {
                //    LabelingJobName = "TestSegment"
                //});

                var labels = context.ClassNames.Select(className => new {label = className});
                

                var labelBody = "{\"document-version\":\"2018-11-28\",\"labels\": " + JsonConvert.SerializeObject(labels) + "}";
                var labelLocation = $"{context.SceneProvisioningJobWorkspace}SegmentationLabels.json";

                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = labelLocation.S3Bucket(),
                    Key = labelLocation.S3Key(),
                    ContentType = "application/json",
                    ContentBody = labelBody
                });

                //try
                //{
                    await sageMaker.CreateLabelingJobAsync(new CreateLabelingJobRequest
                    {
                        Tags = new AutoConstructedList<Tag>
                        {
                            new Tag
                            {
                                Key = "SceneProvisioningJobId",
                                Value = context.SceneProvisioningJobId
                            }
                        },
                        LabelingJobName = $"{context.SceneProvisioningJobId}",
                        LabelAttributeName = "Polygon-ref",
                        LabelCategoryConfigS3Uri = labelLocation,
                        StoppingConditions = new LabelingJobStoppingConditions
                        {
                            MaxPercentageOfInputDatasetLabeled = 100
                        },
                        RoleArn = context.LabelingRoleArn,
                        
                        HumanTaskConfig = new HumanTaskConfig
                        {
                            TaskKeywords = new List<string>
                            {
                                "Images",
                                "image segmentation"
                            },
                            PreHumanTaskLambdaArn = $"arn:aws:lambda:{context.Region}:432418664414:function:PRE-SemanticSegmentation",
                            TaskAvailabilityLifetimeInSeconds = 345600,
                            TaskTimeLimitInSeconds = 300,
                            //MaxConcurrentTaskCount = 1000,
                            AnnotationConsolidationConfig = new AnnotationConsolidationConfig
                            {
                                AnnotationConsolidationLambdaArn =
                                    $"arn:aws:lambda:{context.Region}:432418664414:function:ACS-SemanticSegmentation"
                            },
                            NumberOfHumanWorkersPerDataObject = 1,
                            TaskTitle = "Semantic segmentation",
                            TaskDescription = $"Draw a polygon on objects.",
                            WorkteamArn = context.WorkteamArn,
                            UiConfig = new UiConfig
                            {
                                UiTemplateS3Uri = context.UiTemplateLocation
                            }

                        },
                        InputConfig = new LabelingJobInputConfig
                        {
                            DataSource = new LabelingJobDataSource
                            {
                                S3DataSource = new LabelingJobS3DataSource
                                {
                                    ManifestS3Uri = context.InputManifestLocation
                                }
                            }
                        },
                        OutputConfig = new LabelingJobOutputConfig
                        {
                            S3OutputPath = $"{context.SceneProvisioningJobWorkspace}output/"
                        }
                    });
                    /*
                }
                catch (Exception e)
                {
                    Console.Write(e);
                    if (e.InnerException is HttpErrorResponseException sme)
                    {
                        var stream = sme.Response.ResponseBody.OpenResponse();
                        var sr = new StreamReader(stream);
                        var text = sr.ReadToEnd();
                        Console.WriteLine(text);
                    }
                }
                */
                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "*")]
        [FunctionMemory(Memory = 3008)]
        [FunctionTimeout(Timeout = 900)]
        public sealed class ExtractPolygons : TaskState<Context, CheckIfBackgroundSceneExists>
        {
            readonly IAmazonS3 s3 = new AmazonS3Client();
            readonly IAmazonSQS sqs = new AmazonSQSClient();

            private readonly List<Task<ListObjectsResponse>> listTasks = new List<Task<ListObjectsResponse>>();
        
            
            public override async Task<Context> Execute(Context context)
            {
                var pngPath = $"{context.SceneProvisioningJobWorkspace.S3Key()}output/{context.SceneProvisioningJobId}/annotations/consolidated-annotation/output/";
                var jsonPath = $"{context.SceneProvisioningJobWorkspace.S3Key()}output/{context.SceneProvisioningJobId}/annotations/consolidated-annotation/consolidation-request/iteration-1/";
               

                listTasks.Add(s3.ListObjectsAsync(new ListObjectsRequest
                {
                    BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                    Prefix = pngPath
                }));
                listTasks.Add(s3.ListObjectsAsync(new ListObjectsRequest
                {
                    BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                    Prefix = jsonPath
                }));

                await Task.WhenAll(listTasks);

                var pngObject = listTasks[0].Result.S3Objects.Single();
                var jsonObject = listTasks[1].Result.S3Objects.Single();
                
                var pngResp = await s3.GetObjectAsync(new GetObjectRequest
                {
                    Key = pngObject.Key,
                    BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                });
                var jpgResp = await s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = context.SceneImageLocation.S3Bucket(),
                    Key = context.SceneImageLocation.S3Key()
                });
                var jsonResp = await s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                    Key = jsonObject.Key
                });

                var png = Image.Load(pngResp.ResponseStream, new PngDecoder());
                var image = Image.Load(jpgResp.ResponseStream);

                if (context.GenerateSceneBackground)
                {
                    Console.WriteLine("Making full mask PNG.");
                    var fullMask = new Image<Rgba32>(png.Width, png.Height);
                    for (var x = 0; x < png.Width; x++)
                    {
                        for (var y = 0; y < png.Height; y++)
                        {
                            var pixel = png.Frames[0][x, y];
                            if (!(pixel.R == 255 &&
                                  pixel.G == 255 &&
                                  pixel.B == 255 &&
                                  pixel.R == 255))
                            {
                                var x1 = x;
                                var y1 = y;
                                var pixelToCopy = image.Clone(i => i.Crop(new Rectangle(x1, y1, 1, 1)));

                                fullMask.Mutate(m => m.DrawImage(pixelToCopy, 1, new Point(x1, y1)));
                            }
                        }
                    }

                    using (var stream = new MemoryStream())
                    {
                        fullMask.Save(stream, new PngEncoder());
                        await s3.PutObjectAsync(new PutObjectRequest
                        {
                            BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                            Key = $"{context.SceneProvisioningJobWorkspace.S3Key()}full-mask.png",
                            ContentType = "image/png",
                            InputStream = stream
                        });
                    }

                    context.BackgroundImagePercentComplete = 10;
                    context.SceneBackgroundLocation = context.SceneProvisioningJobWorkspace + "scene-background.jpg";

                    await sqs.SendMessageAsync(new SendMessageRequest
                    {
                        QueueUrl = context.SceneBackgroundGenerationQueueUrl,
                        MessageBody = JsonConvert.SerializeObject(context)
                    });
                }

                var colorMappings = new Dictionary<string, Rgba32>();
                using (var sr = new StreamReader(jsonResp.ResponseStream))
                {
                    var json = await sr.ReadToEndAsync();
                    var segmentConfig = JsonConvert.DeserializeObject<dynamic>(json);
                    var content = segmentConfig[0].annotations[0].annotationData.content;
                    var contentObj = JsonConvert.DeserializeObject<dynamic>(content.Value);
                    foreach (var mapping in contentObj["crowd-semantic-segmentation"].labelMappings)
                    {
                        string className = mapping.Name;
                        string hexColor = mapping.Value.color.Value.Replace("#", string.Empty);
                        var r = int.Parse(hexColor.Substring(0, 2), System.Globalization.NumberStyles.HexNumber);
                        var g = int.Parse(hexColor.Substring(2, 2), System.Globalization.NumberStyles.HexNumber);
                        var b = int.Parse(hexColor.Substring(4, 2), System.Globalization.NumberStyles.HexNumber);
                        var color = new Rgba32
                        {
                            R = Convert.ToByte(r),
                            G = Convert.ToByte(g),
                            B = Convert.ToByte(b),
                            A = Convert.ToByte(1)
                        };
                        colorMappings.Add(className, color);
                    }
                }

                var classObjectLocationBuilder = new StringBuilder();
                classObjectLocationBuilder.AppendLine("class,x,y,width,height");

                foreach (var colorMapping in colorMappings)
                {
                    var file = new Image<Rgba32>(png.Width, png.Height);
                    int maxY = 0;
                    int minX = 1000000;
                    int minY = 1000000;
                    int maxX = 0;
                    for (var x = 0; x < png.Width; x++)
                    {
                        for (var y = 0; y < png.Height; y++)
                        {
                            var pixel = png.Frames[0][x, y];
                            if (pixel.R == colorMapping.Value.R && 
                                pixel.G == colorMapping.Value.G && 
                                pixel.B == colorMapping.Value.B)
                            {
                                var x1 = x;
                                var y1 = y;
                                if (y > maxY)
                                    maxY = y;
                                if (x > maxX)
                                    maxX = x;
                                if (x < minX)
                                    minX = x;
                                if (y < minY)
                                    minY = y;
                                var pixelToCopy = image.Clone(i => i.Crop(new Rectangle(x1, y1, 1, 1)));
                                file.Mutate(m => m.DrawImage(pixelToCopy, 1, new Point(x1, y1)));
                            }
                        }
                    }
                    var width = maxX - minX;
                    var height = maxY - minY;
                    file.Mutate(c => c.Crop(new Rectangle(minX, minY, width, height)));
                    //file.Save($"{colorMapping.Key}.png");

                    classObjectLocationBuilder.AppendLine($"{colorMapping.Key},{minX},{minY},{width},{height}");

                    using (var stream = new MemoryStream())
                    {
                        file.Save(stream, new PngEncoder());
                        await s3.PutObjectAsync(new PutObjectRequest
                        {
                            BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                            Key = $"{context.SceneProvisioningJobWorkspace.S3Key()}objects/{colorMapping.Key}.png",
                            ContentType = "image/png",
                            InputStream = stream
                        });
                    }

                    file.Dispose();
                }

                image.Dispose();
                png.Dispose();

                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                    Key = $"{context.SceneProvisioningJobWorkspace.S3Key()}object-locations.csv",
                    ContentType = "text/csv",
                    ContentBody = classObjectLocationBuilder.ToString()
                });

                return context;
            }
        }

        public sealed class CheckIfBackgroundSceneExists : ChoiceState<MakeTrainingImages>
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<WaitBeforeCheckingBackgroundJobExistence, Context>(c => c.BackgroundImagePercentComplete < 100)
            };
        }

        public sealed class WaitBeforeCheckingBackgroundJobExistence : WaitState<GetBackgroundImage>
        {
            public override int Seconds => 60;
        }

        [DotStep.Core.Action(ActionName = "*")]
        public sealed class GetBackgroundImage : TaskState<Context, CheckIfBackgroundSceneExists>
        {
            IAmazonS3 s3 = new AmazonS3Client();

            public override async Task<Context> Execute(Context context)
            {
                try
                {
                    await s3.GetObjectAsync(new GetObjectRequest
                    {
                        BucketName = context.SceneBackgroundLocation.S3Bucket(),
                        Key = context.SceneBackgroundLocation.S3Key()
                    });

                    // if we make it this far the image exists.
                    context.BackgroundImagePercentComplete = 100;
                }
                catch (AmazonS3Exception s3Exception)
                {
                    if (s3Exception.ErrorCode == "NoSuchKey")
                        context.BackgroundImagePercentComplete = 20;
                    else throw;
                }
                
                return context;
            }
        }


        public sealed class CheckTrainingJobStatus : ChoiceState<CreateOrUpdateEndpoint>
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<WaitForTrainingJob, Context>(c => c.TrainingJobPercentComplete < 100)
            };
        }

        public sealed class WaitForTrainingJob : WaitState<GetTrainingJobStatus>
        {
            public override int Seconds => 240;
        }

        [DotStep.Core.Action(ActionName = "*")]
        public sealed class GetTrainingJobStatus : TaskState<Context, CheckTrainingJobStatus>
        {
            IAmazonSageMaker sageMaker = new AmazonSageMakerClient();
            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.DescribeTrainingJobAsync(new DescribeTrainingJobRequest
                {
                    TrainingJobName = context.SceneCode + "-" + context.SceneProvisioningJobId
                });
                switch (result.TrainingJobStatus.Value)
                {
                    case "InProgress":
                        context.TrainingJobPercentComplete = 20;
                        break;
                    case "Completed":
                        context.TrainingJobPercentComplete = 100;
                        break;
                    default:
                        throw new Exception($"Unsupported training status: {result.TrainingJobStatus.Value}");
                }

                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "*")]
        public sealed class GetEndpointStatus : TaskState<Context, CheckEndpointStatus>
        {
            IAmazonSageMaker sageMaker = new AmazonSageMakerClient();
            public override async Task<Context> Execute(Context context)
            {

                var result = await sageMaker.DescribeEndpointAsync(new DescribeEndpointRequest
                {
                    EndpointName = context.SceneCode
                });

                switch (result.EndpointStatus.Value)
                {
                    case "Creating":
                        context.EndpointPercentComplete = 20;
                        break;
                    case "InService":
                        context.EndpointPercentComplete = 100;
                        break;
                    default:
                        throw new Exception($"Unsupported endpoint status: {result.EndpointStatus.Value}");
                }
                
                return context;
            }
        }

        public sealed class CheckEndpointStatus : ChoiceState<CreateDashboard>
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<WaitForEndpoint, Context>(c => c.EndpointPercentComplete < 100)
            };
        }

        public sealed class WaitForEndpoint : WaitState<GetEndpointStatus>
        {
            public override int Seconds => 240;
        }

        [DotStep.Core.Action(ActionName = "*")]
        public sealed class CreateTrainingJob : TaskState<Context, CheckTrainingJobStatus>
        {
            IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.CreateTrainingJobAsync(new CreateTrainingJobRequest
                {
                    Tags = new AutoConstructedList<Tag>
                    {
                        new Tag
                        {
                            Key = "SceneCode",
                            Value = context.SceneCode
                        },
                        new Tag
                        {
                            Key = "SceneProvisioningJobId",
                            Value = context.SceneProvisioningJobId
                        }
                    },
                    HyperParameters = new Dictionary<string, string>
                    {
                        {"base_network", "vgg-16" },
                        {"epochs", "40" },
                        {"image_shape", "300" },
                        {"label_width", "350" },
                        {"learning_rate", "0.001" },
                        {"lr_scheduler_factor", "0.1" },
                        {"mini_batch_size", "32" },
                        {"momentum", "0.9" },
                        {"nms_threshold", "0.45" },
                        {"num_classes", Convert.ToString(context.ClassNames.Count) },
                        {"num_training_samples", Convert.ToString(context.NumberOfTrainingSamples) },
                        {"optimizer", "sgd" },
                        {"overlap_threshold", "0.5" },
                        {"use_pretrained_model", "1" },
                        {"weight_decay", "0.0005" }
                    },
                    ResourceConfig = new ResourceConfig
                    {
                        InstanceCount = 1,
                        InstanceType = TrainingInstanceType.MlP32xlarge,
                        VolumeSizeInGB = 256
                    },
                    RoleArn = context.LabelingRoleArn,
                    StoppingCondition = new StoppingCondition
                    {
                        MaxRuntimeInSeconds = Convert.ToInt32(TimeSpan.FromHours(8).TotalSeconds)
                    },
                    OutputDataConfig = new OutputDataConfig
                    {
                        S3OutputPath = context.SceneProvisioningJobWorkspace + "object-detection/"
                    },
                    TrainingJobName = $"{context.SceneCode}-{context.SceneProvisioningJobId}",
                    AlgorithmSpecification = new AlgorithmSpecification
                    {
                        TrainingInputMode = new TrainingInputMode("File"),
                        TrainingImage = "811284229777.dkr.ecr.us-east-1.amazonaws.com/object-detection:latest"
                    },
                    InputDataConfig = new List<Channel>
                    {
                        new Channel
                        {
                            ChannelName = "train",
                            ContentType = "application/x-image",
                            InputMode = TrainingInputMode.File,
                            DataSource = new DataSource
                            {
                                S3DataSource = new S3DataSource
                                {
                                    S3DataDistributionType = S3DataDistribution.FullyReplicated,
                                    S3DataType = new S3DataType("S3Prefix"),
                                    S3Uri = $"{context.SceneProvisioningJobWorkspace}object-detection/train/images"
                                }
                            }
                        },
                        new Channel
                        {
                            ChannelName = "validation",
                            ContentType = "application/x-image",
                            InputMode = TrainingInputMode.File,
                            DataSource = new DataSource
                            {
                                S3DataSource = new S3DataSource
                                {
                                    S3DataDistributionType = S3DataDistribution.FullyReplicated,
                                    S3DataType = new S3DataType("S3Prefix"),
                                    S3Uri = $"{context.SceneProvisioningJobWorkspace}object-detection/validation/images"
                                }
                            }
                        },
                        new Channel
                        {
                            ChannelName = "train_annotation",
                            ContentType = "application/x-image",
                            InputMode = TrainingInputMode.File,
                            DataSource = new DataSource
                            {
                                S3DataSource = new S3DataSource
                                {
                                    S3DataDistributionType = S3DataDistribution.FullyReplicated,
                                    S3DataType = new S3DataType("S3Prefix"),
                                    S3Uri = $"{context.SceneProvisioningJobWorkspace}object-detection/train/annotations"
                                }
                            }
                        },
                        new Channel
                        {
                            ChannelName = "validation_annotation",
                            ContentType = "application/x-image",
                            InputMode = TrainingInputMode.File,
                            DataSource = new DataSource
                            {
                                S3DataSource = new S3DataSource
                                {
                                    S3DataDistributionType = S3DataDistribution.FullyReplicated,
                                    S3DataType = new S3DataType("S3Prefix"),
                                    S3Uri = $"{context.SceneProvisioningJobWorkspace}object-detection/validation/annotations"
                                }
                            }
                        },
                    }
                });
                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "*")]
        [FunctionMemory(Memory = 1024)]
        [FunctionTimeout(Timeout = 900)]
        public sealed class MakeTrainingImages : TaskState<Context, CreateTrainingJob>
        {
            class Location
            {
                public int x { get; set; }
                public int y { get; set; }
                public int w { get; set; }
                public int h { get; set; }
            }

            IAmazonS3 s3 = new AmazonS3Client();

            private async Task BuildObjectDetectionData(Context context, Image<Rgba32> backgroundScene, string objectLocationsCsv)
            {

                var pngs = new List<Image<Rgba32>>();

                var locations = new Dictionary<string, Location>();

                var random = new Random();
                
                foreach (var className in context.ClassNames)
                {
                    var getResult = await s3.GetObjectAsync(context.SceneProvisioningJobWorkspace.S3Bucket(),
                        $"{context.SceneProvisioningJobWorkspace.S3Key()}objects/{className}.png");

                    var png = Image.Load(getResult.ResponseStream, new PngDecoder());

                    pngs.Add(png);

                    var location = new Location();

                    foreach (var row in objectLocationsCsv.Split('\n'))
                        if (row.Split(',')[0] == className)
                        {
                            location.x = Convert.ToInt32(row.Split(',')[1]);
                            location.y = Convert.ToInt32(row.Split(',')[2]);
                            location.w = Convert.ToInt32(row.Split(',')[3]);
                            location.h = Convert.ToInt32(row.Split(',')[4]);
                        }

                    locations.Add(className, location);


                    //background.Mutate(b => b.DrawImage(png, 1, new Point(x, y)));
                }

                var xVar = Convert.ToInt32(0.02m * backgroundScene.Width);
                var yVar = Convert.ToInt32(0.02m * backgroundScene.Height);

                var putTasks = new List<Task<PutObjectResponse>>();
                for (int i = 0; i < 200; i++)
                {

                    var background = backgroundScene.Clone();
                    var channel = random.Next(0, 9) < 2 ? "validation" : "train";

                    var b = new StringBuilder();
                    b.AppendLine("{\"file\":\"" + i + ".jpg\", \"image_size\": [{ \"width\": " +
                                 background.Width + ", \"height\": " + background.Height +
                                 ", \"depth\": 3 }], \"annotations\": [");

                    var classIndex = 0;
                    foreach (var className in context.ClassNames)
                    {
                        var l = locations[className];

                        var randomX = random.Next(-1 * xVar, xVar) + l.x;
                        var randomY = random.Next(-1 * yVar, yVar) + l.y;

                        if (randomX < 0)
                            randomX = 0;
                        if (randomY < 0)
                            randomY = 0;
                        

                        b.AppendLine("{\"class_id\": " + classIndex + ", \"left\": " + randomX + ", \"top\": " + randomY +
                                     ", \"width\": " + l.w + ", \"height\": " + l.h + "}");

                        if (classIndex < context.ClassNames.Count - 1)
                            b.Append(",");
                        
                        var ci = classIndex;
                        background.Mutate(bg => bg.DrawImage(pngs[ci], 1, new Point(randomX, randomY)));

                        classIndex++;
                    }
                    b.AppendLine("]}");
                    

                    var json = b.ToString();
                    var deserializeObject = JsonConvert.DeserializeObject(json);
                    var formattedJson = JsonConvert.SerializeObject(deserializeObject, Formatting.Indented);
                    
                    putTasks.Add(s3.PutObjectAsync(
                        new PutObjectRequest
                        {
                            BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                            Key =
                                $"{context.SceneProvisioningJobWorkspace.S3Key()}object-detection/{channel}/annotations/{i}.json",
                            ContentType = "application/json",
                            ContentBody = formattedJson
                        }));

                    using (var stream = new MemoryStream())
                    {
                        background.Save(stream, new JpegEncoder());
                        await s3.PutObjectAsync(
                            new PutObjectRequest
                            {
                                BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                                Key = $"{context.SceneProvisioningJobWorkspace.S3Key()}object-detection/{channel}/images/{i}.jpg",
                                ContentType = "image/jpg",
                                InputStream = stream
                            });
                    }

                }

                await Task.WhenAll(putTasks);

            }

            public override async Task<Context> Execute(Context context)
            {
                var backgroundResult = await s3.GetObjectAsync(context.SceneBackgroundLocation.S3Bucket(),
                    context.SceneBackgroundLocation.S3Key());
                var objectLocations = await s3.GetObjectAsync(context.SceneProvisioningJobWorkspace.S3Bucket(),
                    context.SceneProvisioningJobWorkspace.S3Key() + "object-locations.csv");

                var background = Image.Load(backgroundResult.ResponseStream, new JpegDecoder());
                string objectLocationsCsv;

                using (var sr = new StreamReader(objectLocations.ResponseStream))
                  objectLocationsCsv = sr.ReadToEnd();

                var objectDetectionTask = BuildObjectDetectionData(context, background, objectLocationsCsv);


                var trainList = new StringBuilder();
                var validationList = new StringBuilder();

                var itemIndex = 0;
                var classIndex = 0;
                foreach (var className in context.ClassNames)
                {
                    Console.WriteLine($"Processing class {className}");

                    int x = 0;
                    int y = 0;
                    
                    foreach (var row in objectLocationsCsv.Split('\n'))
                        if (row.Split(',')[0] == className)
                        {
                            x = Convert.ToInt32(row.Split(',')[1]);
                            y = Convert.ToInt32(row.Split(',')[2]);
                        }

                    
                    var getResult = await s3.GetObjectAsync(context.SceneProvisioningJobWorkspace.S3Bucket(),
                        $"{context.SceneProvisioningJobWorkspace.S3Key()}objects/{className}.png");

                    var png = Image.Load(getResult.ResponseStream, new PngDecoder());

                    var maxLength = (new List<int> {png.Width, png.Height}).Max();

                    var xPadding = x / 20;
                    var yPadding = y / 20;

                    var outerX = x - xPadding;
                    var outerY = y - yPadding;
                    var width = png.Width + xPadding * 2;
                    var height = png.Height + yPadding * 2;


                    var random = new Random();

                    var classBackground = background
                        .Clone(img => img.Crop(new Rectangle(
                            outerX,
                            outerY,
                            width,
                           height)));

                    var trainingBackground = classBackground.Clone(i => i.Resize(122, 122));
                    
                    using (var stream = new MemoryStream())
                    {
                        trainingBackground.Save(stream, new JpegEncoder());
                        await s3.PutObjectAsync(
                            new PutObjectRequest
                            {
                                BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                                Key = $"{context.SceneProvisioningJobWorkspace.S3Key()}image-classification/train/{className}Background/0.jpg",
                                ContentType = "image/jpg",
                                InputStream = stream
                            });
                    }
                    using (var stream = new MemoryStream())
                    {
                        trainingBackground.Save(stream, new JpegEncoder());
                        await s3.PutObjectAsync(
                            new PutObjectRequest
                            {
                                BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                                Key = $"{context.SceneProvisioningJobWorkspace.S3Key()}image-classification/validation/{className}Background/0.jpg",
                                ContentType = "image/jpg",
                                InputStream = stream
                            });
                    }

                    for (int i = 0; i < context.ImagesGeneratedPerClass; i++)
                    {
                        var drawX = random.Next(0, width / 2);
                        var drawY = random.Next(0, height / 2);

                        var trainingImage = classBackground
                            .Clone(img => img.DrawImage(png, 1, new Point(drawX, drawY)));

                        trainingImage.Mutate(img => img.Resize(122, 122));

                        //trainingImage.Save($"classes-{className}-{i}.jpg", new JpegEncoder());

                        var channel = random.Next(0, 9) < 2 ? "validation" : "train";


                        using (var stream = new MemoryStream())
                        {
                            trainingImage.Save(stream, new JpegEncoder());
                            await s3.PutObjectAsync(
                                new PutObjectRequest
                                {
                                    BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                                    Key = $"{context.SceneProvisioningJobWorkspace.S3Key()}image-classification/{channel}/{className}/{i}.jpg",
                                    ContentType = "image/jpg",
                                    InputStream = stream
                                });
                        }

                        var line = $"{itemIndex}\t{classIndex}\t{className}/{i}.jpg";
                        var backgroundLine = $"{itemIndex + 10000}\t{classIndex + context.ClassNames.Count}\t{className}Background/0.jpg";

                        if (channel == "train")
                        {
                            trainList.AppendLine(line);
                            context.NumberOfTrainingSamples++;

                            trainList.AppendLine(backgroundLine);
                            context.NumberOfTrainingSamples++;
                        }
                        else
                            validationList.AppendLine(line);

                        itemIndex++;
                    }
                    //context.NumberOfTrainingSamples++; // note: +1 for the background training class image we added.
                    //trainList.AppendLine($"{10000 + classIndex + itemIndex}\t{classIndex + context.ClassNames.Count}\t{className}Background/0.jpg");
                    //validationList.AppendLine($"{10000 + classIndex + itemIndex}\t{classIndex + context.ClassNames.Count}\t{className}Background/0.jpg");
                    classIndex++;
                }

                

                await s3.PutObjectAsync(
                    new PutObjectRequest
                    {
                        BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                        Key = $"{context.SceneProvisioningJobWorkspace.S3Key()}image-classification/train.lst",
                        ContentType = "text/tsv",
                        ContentBody = trainList.ToString()
                    });
                await s3.PutObjectAsync(
                    new PutObjectRequest
                    {
                        BucketName = context.SceneProvisioningJobWorkspace.S3Bucket(),
                        Key = $"{context.SceneProvisioningJobWorkspace.S3Key()}image-classification/validation.lst",
                        ContentType = "text/tsv",
                        ContentBody = validationList.ToString()
                    });

                await Task.WhenAll(objectDetectionTask);

                return context;
            }

      
        }

        [DotStep.Core.Action(ActionName = "*")]
        public sealed class CreateDashboard : TaskState<Context, Done>
        {
            IAmazonCloudWatch cloudWatch = new AmazonCloudWatchClient();
            public override async Task<Context> Execute(Context context)
            {
                var dashboard = new Dashboard
                {
                    Widgets = new List<Widget>
                    {
                        new Widget
                        {
                            Type = "metric",
                            X = 0,
                            Y = 0,
                            Width = 24,
                            Height = 9,
                            Properties = new Properties
                            {
                               Metrics = new AutoConstructedList<List<string>>(),
                                View = "timeSeries",
                                Stacked = false,
                                Region = context.Region,
                                Stat = "Maximum",
                                Period = 10,
                                Title = context.SceneCode,
                                YAxis = new Axis
                                {
                                    Left = new Left
                                    {
                                        Max = 100
                                    }
                                }
                            }
                        }
                    }
                };

                dashboard.Widgets[0].Properties.Metrics.Add(new List<string>
                {
                    "Cameras", "Confidence", "CameraKey", context.CameraKey, "Label", "Person", "Source", "Rekognition"
                });

                foreach (var className in context.ClassNames)
                {
                    dashboard.Widgets[0].Properties.Metrics.Add(new List<string>
                    {
                        "Cameras", "Confidence", "CameraKey", context.CameraKey, "Label", className, "Source", context.SceneCode
                    });
                }
                
                var json = JsonConvert.SerializeObject(dashboard, Formatting.Indented);

                var putResult = await cloudWatch.PutDashboardAsync(new PutDashboardRequest
                {
                    DashboardName = context.SceneCode,
                    DashboardBody = json
                });

                return context;
            }


            public class Dashboard
            {
                [JsonProperty("widgets")] 
                public List<Widget> Widgets { get; set; }
            }

            public class Widget
            {
                [JsonProperty("type")]
                public string Type { get; set; }
                [JsonProperty("x")]
                public int X { get; set; }
                [JsonProperty("y")]
                public int Y { get; set; }
                [JsonProperty("width")]
                public int Width { get; set; }
                [JsonProperty("height")]
                public int Height { get; set; }
                [JsonProperty("properties")]
                public Properties Properties { get; set; }
            }

            public class Properties
            {
                [JsonProperty("metrics")]
                public List<List<string>> Metrics { get; set; }
                [JsonProperty("view")]
                public string View { get; set; }
                [JsonProperty("stacked")]
                public bool Stacked { get; set; }
                [JsonProperty("region")]
                public string Region { get; set; }
                [JsonProperty("stat")]
                public string Stat { get; set; }
                [JsonProperty("period")]
                public int Period { get; set; }
                [JsonProperty("title")]
                public string Title { get; set; }
                [JsonProperty("yAxis")]
                public Axis YAxis { get; set; }

            }

            public class Axis
            {
                [JsonProperty("left")]
                public Left Left { get; set; }
            }

            public class Left
            {
                [JsonProperty("max")]
                public int Max { get; set; }
            }

        }


        [DotStep.Core.Action(ActionName = "*")]
        public sealed class CreateOrUpdateEndpoint : TaskState<Context, CheckEndpointStatus>
        {
            IAmazonSageMaker sageMaker = new AmazonSageMakerClient();
            IAmazonS3 s3 = new AmazonS3Client();
            IAmazonSimpleSystemsManagement ssm = new AmazonSimpleSystemsManagementClient();

            public override async Task<Context> Execute(Context context)
            {
                var modelLocation =
                    $"{context.SceneProvisioningJobWorkspace}object-detection/{context.SceneCode}-{context.SceneProvisioningJobId}/output/model.tar.gz";

                var tags = new AutoConstructedList<Tag>
                {
                    new Tag
                    {
                        Key = "SceneCode",
                        Value = context.SceneCode
                    },
                    new Tag
                    {
                        Key = "SceneProvisioningJobId",
                        Value = context.SceneProvisioningJobId
                    }
                };

                var createModelResponse = await sageMaker.CreateModelAsync(new CreateModelRequest
                {
                    ExecutionRoleArn = context.LabelingRoleArn,
                    ModelName = $"{context.SceneCode}-{context.SceneProvisioningJobId}",
                    PrimaryContainer = new ContainerDefinition
                    {
                        Image = "811284229777.dkr.ecr.us-east-1.amazonaws.com/object-detection:latest",
                        ModelDataUrl = modelLocation
                    },
                    Tags = tags
                });


                var createEndpointConfigResp =
                    await sageMaker.CreateEndpointConfigAsync(new CreateEndpointConfigRequest
                    {
                        EndpointConfigName = context.SceneCode + "-" + context.SceneProvisioningJobId,
                        ProductionVariants = new List<ProductionVariant>
                        {
                            new ProductionVariant
                            {
                                InitialInstanceCount = 1,
                                InitialVariantWeight = 1F,
                                VariantName = "AllTraffic",
                                ModelName = context.SceneCode + "-" + context.SceneProvisioningJobId,
                                InstanceType = await GetProductionVariantInstanceType(modelLocation)
                            }
                        },
                        Tags = tags
                    });

                try
                {
                    var createEndpointResp = await sageMaker.CreateEndpointAsync(new CreateEndpointRequest
                    {
                        Tags = tags,
                        EndpointConfigName = context.SceneCode + "-" + context.SceneProvisioningJobId,
                        EndpointName = context.SceneCode
                    });
                }
                catch (Exception e)
                {
                    if (true)
                    {
                        await sageMaker.UpdateEndpointAsync(new UpdateEndpointRequest
                        {
                            EndpointConfigName = context.SceneCode + "-" + context.SceneProvisioningJobId,
                            EndpointName = context.SceneCode
                        });
                    }
                }


                await ssm.PutParameterAsync(new PutParameterRequest
                {
                    Name = $"/Cameras/{context.CameraKey}/Enabled",
                    Value = "True",
                    Type = Amazon.SimpleSystemsManagement.ParameterType.String,
                    Overwrite = true
                });
                
                return context;
            }
            
            public async Task<ProductionVariantInstanceType>  GetProductionVariantInstanceType(string modelLocation)
            {
                var metadata = await s3.GetObjectMetadataAsync(
                    new GetObjectMetadataRequest
                    {
                        BucketName = modelLocation.S3Bucket(),
                        Key = modelLocation.S3Key()
                    });
                var modelSizeInBytes = metadata.Headers.ContentLength;
                var modelSizeInGbs = modelSizeInBytes / 1000000000;

                if (modelSizeInGbs < 5)
                    return ProductionVariantInstanceType.MlC5Large;
                if (modelSizeInGbs < 10)
                    return ProductionVariantInstanceType.MlC52xlarge;
                if (modelSizeInGbs < 20)
                    return ProductionVariantInstanceType.MlC54xlarge;
                if (modelSizeInGbs < 40)
                    return ProductionVariantInstanceType.MlC59xlarge;
                if (modelSizeInGbs < 120)
                    return ProductionVariantInstanceType.MlC518xlarge;
                throw new Exception($"Model size too big, {modelSizeInGbs} GBs.");
            }
            
        }

        public sealed class Done : PassState
        {
            public override bool End => true;
        }
    }
}