using Microsoft.Azure.Cosmos.Spatial;
using WebReaper.Builders;
using WebReaper.Sinks.Models;
using Xunit.Abstractions;

namespace WebReaper.UnitTests
{
    public class EngineTests
    {
        private readonly ITestOutputHelper _output;

        public EngineTests(ITestOutputHelper output) => _output = output;

        [Fact]
        public async Task EngineOneTaskAndCancelTest()
        {
            string exampleUrl = "https://devblogs.microsoft.com/dotnet/announcing-dotnet-10/";
            var engine = await new ScraperEngineBuilder()
                .Get(exampleUrl)
                .Parse(
                    new()
                    {
                        new(
                            "button",
                            ".ytp-large-play-button.ytp-button.ytp-large-play-button-red-bg"
                        ),
                    }
                )
                .AddSink(new YoutubeButtonSink(exampleUrl, _output) { DataCleanupOnStart = true })
                .LogToConsole()
                .BuildAsync();

            try
            {
                await Task.WhenAll(
                    Task.Run(async () =>
                    {
                        await engine.RunAsync();
                    }),
                    Task.Run(async () =>
                    {
                        while (await engine.HasScheduledJobs())
                        {
                            await Task.Delay(200);
                        }
                        await engine.StopAsync();
                    })
                );
            }
            catch (Exception ex)
            {
                Assert.True(ex is OperationCanceledException);
            }
            Assert.False(engine.IsRunning);
        }

        [Fact]
        public async Task EngineOneTaskAndCancelWithCtsTest()
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            string exampleUrl = "https://devblogs.microsoft.com/dotnet/announcing-dotnet-10/";
            var engine = await new ScraperEngineBuilder()
                .Get(exampleUrl)
                .Parse(
                    new()
                    {
                        new(
                            "button",
                            ".ytp-large-play-button.ytp-button.ytp-large-play-button-red-bg"
                        ),
                    }
                )
                .WithCancellationTokenSource(cts)
                .AddSink(new YoutubeButtonSink(exampleUrl, _output) { DataCleanupOnStart = true })
                .LogToConsole()
                .BuildAsync();

            try
            {
                await Task.WhenAll(
                    Task.Run(async () =>
                    {
                        await engine.RunAsync();
                    }),
                    Task.Run(async () =>
                    {
                        while (await engine.HasScheduledJobs())
                        {
                            await Task.Delay(200);
                        }
                        await cts.CancelAsync();
                    })
                );
            }
            catch (Exception ex)
            {
                Assert.True(ex is OperationCanceledException);
            }
            Assert.True(cts.IsCancellationRequested);
            Assert.False(engine.IsRunning);
        }

        [Fact]
        public async Task EngineAddMoreWorkTest()
        {
            int moreWorkCount = 5;
            string exampleUrl = "https://devblogs.microsoft.com/dotnet/announcing-dotnet-10/";
            var engine = await new ScraperEngineBuilder()
                .Get(exampleUrl)
                .Parse(
                    new()
                    {
                        new(
                            "button",
                            ".ytp-large-play-button.ytp-button.ytp-large-play-button-red-bg"
                        ),
                    }
                )
                .AddSink(new YoutubeButtonSink(exampleUrl, _output) { DataCleanupOnStart = true })
                .LogToConsole()
                .BuildAsync();

            try
            {
                await Task.WhenAll(
                    Task.Run(async () =>
                    {
                        await engine.RunAsync();
                    }),
                    Task.Run(async () =>
                    {
                        for (int i = 0; i < moreWorkCount; i++)
                        {
                            await Task.Delay(500);
                            while (await engine.HasScheduledJobs())
                            {
                                await Task.Delay(200);
                            }
                            ConfigBuilder builder = new ConfigBuilder();
                            builder
                                .Get(exampleUrl)
                                .WithScheme(
                                    new()
                                    {
                                        new(
                                            "button",
                                            ".ytp-large-play-button.ytp-button.ytp-large-play-button-red-bg"
                                        ),
                                    }
                                );
                            _ = Task.Factory.StartNew(async () =>
                                await engine.ReconfigureAsync(builder.Build())
                            );
                        }
                        await engine.StopAsync();
                    })
                );
            }
            catch (Exception ex)
            {
                Assert.True(ex is OperationCanceledException);
            }
            Assert.False(engine.IsRunning);
        }

        internal class YoutubeButtonSink(string url, ITestOutputHelper output)
            : WebReaper.Sinks.Abstract.IScraperSink
        {
            public bool DataCleanupOnStart { get; set; }

            public async Task EmitAsync(
                ParsedData entity,
                CancellationToken cancellationToken = default
            )
            {
                output.WriteLine($"Processing URL: {url}");
                await Task.Delay(1000);
            }
        }
    }
}
