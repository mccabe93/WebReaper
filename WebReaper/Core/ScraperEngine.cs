using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using WebReaper.ConfigStorage.Abstract;
using WebReaper.Core.Scheduler.Abstract;
using WebReaper.Core.Spider.Abstract;
using WebReaper.Domain;
using WebReaper.Exceptions;
using static WebReaper.Infra.Executor;

namespace WebReaper.Core;

public sealed class ScraperEngine(
    int parallelismDegree,
    CancellationTokenSource cancellationTokenSource,
    IScraperConfigStorage configStorage,
    IScheduler jobScheduler,
    ISpider spider,
    ILogger logger
)
{
    private readonly IScraperConfigStorage ConfigStorage = configStorage;
    private readonly IScheduler Scheduler = jobScheduler;
    private readonly ISpider Spider = spider;
    private readonly ILogger Logger = logger;
    private readonly int ParallelismDegree = parallelismDegree;
    private readonly CancellationTokenSource CancellationTokenSource = cancellationTokenSource;
    private ParallelOptions _parallelism = new() { MaxDegreeOfParallelism = parallelismDegree };

    public bool IsRunning => !CancellationTokenSource.IsCancellationRequested;

    public async Task<bool> HasScheduledJobs()
    {
        return await Scheduler.HasScheduledJobsAsync(CancellationTokenSource.Token);
    }

    public async Task StopAsync()
    {
        await CancellationTokenSource.CancelAsync();
    }

    public async Task ReconfigureAsync(ScraperConfig newConfiguration, bool reinitialize = false)
    {
        await ConfigStorage.CreateConfigAsync(newConfiguration);
        await RunAsync(reinitialize);
    }

    public async Task RunAsync(bool initialize = true)
    {
        if (initialize)
        {
            await Scheduler.Initialization;
            _parallelism = new ParallelOptions { MaxDegreeOfParallelism = ParallelismDegree };
        }

        Logger.LogInformation("Start {class}.{method}", nameof(ScraperEngine), nameof(RunAsync));

        var config = await ConfigStorage.GetConfigAsync();

        foreach (var startUrl in config.StartUrls)
        {
            Logger.LogInformation(
                "Scheduling the initial scraping job with start url {startUrl}",
                startUrl
            );

            await Scheduler.AddAsync(
                new Job(
                    startUrl,
                    config.LinkPathSelectors,
                    ImmutableQueue.Create<string>(),
                    config.StartPageType,
                    config.PageActions
                ),
                CancellationTokenSource.Token
            );
        }

        try
        {
            Logger.LogInformation("Start consuming the scraping jobs");

            await Parallel.ForEachAsync(
                Scheduler.GetAllAsync(CancellationTokenSource.Token),
                _parallelism,
                async (job, token) =>
                {
                    Logger.LogInformation("Start crawling url {Url}", job.Url);

                    var newJobs = await RetryAsync(async () =>
                        await Spider.CrawlAsync(job, CancellationTokenSource.Token)
                    );

                    Logger.LogInformation("Received {JobsCount} new jobs", newJobs.Count);

                    await Scheduler.AddAsync(newJobs, CancellationTokenSource.Token);
                }
            );
        }
        catch (PageCrawlLimitException ex)
        {
            Logger.LogWarning(
                ex,
                "Shutting down due to page crawl limit {Limit}",
                ex.PageCrawlLimit
            );
        }
        catch (TaskCanceledException ex)
        {
            Logger.LogWarning(ex, "Shutting down due to cancellation");
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Shutting down due to unhandled exception");
            throw;
        }
    }
}
