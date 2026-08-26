using System.Diagnostics;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string readyMarker = "MemKafka ready kafka=";
var repositoryRoot = FindRepositoryRoot();
Process? process = null;
try
{
    var bootstrapServers = Environment.GetEnvironmentVariable("MEMKAFKA_BOOTSTRAP_SERVERS");
    if (string.IsNullOrWhiteSpace(bootstrapServers))
    {
        var binaryName = OperatingSystem.IsWindows() ? "memkafka.exe" : "memkafka";
        var binary = Environment.GetEnvironmentVariable("MEMKAFKA_BINARY")
            ?? Path.Combine(repositoryRoot, "target", "debug", binaryName);
        if (!File.Exists(binary))
        {
            throw new FileNotFoundException(
                $"MemKafka binary not found at '{binary}'. Run 'cargo build' first.",
                binary);
        }

        process = StartMemKafka(binary, repositoryRoot);
        bootstrapServers = await ReadBootstrapServers(process);
    }
    Console.WriteLine($"ready  {bootstrapServers}");

    var config = new AdminClientConfig
    {
        BootstrapServers = bootstrapServers,
        SocketTimeoutMs = 5_000,
        ApiVersionRequestTimeoutMs = 5_000,
    };
    using var admin = new AdminClientBuilder(config).Build();

    var autoTopic = $"auto-{Guid.NewGuid():N}";
    var autoMetadata = admin.GetMetadata(autoTopic, TimeSpan.FromSeconds(5));
    AssertTopicPartitions(autoMetadata, autoTopic, 2);
    Console.WriteLine("pass   metadata auto-creates two partitions");

    var explicitTopic = $"explicit-{Guid.NewGuid():N}";
    await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = explicitTopic,
                NumPartitions = 6,
                ReplicationFactor = 1,
            },
        ],
        new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(5) });
    var explicitMetadata = admin.GetMetadata(explicitTopic, TimeSpan.FromSeconds(5));
    AssertTopicPartitions(explicitMetadata, explicitTopic, 6);
    Console.WriteLine("pass   explicit topic has six partitions");

    var invalidTopic = $"invalid-rf-{Guid.NewGuid():N}";
    try
    {
        await admin.CreateTopicsAsync(
            [
                new TopicSpecification
                {
                    Name = invalidTopic,
                    NumPartitions = 2,
                    ReplicationFactor = 2,
                },
            ],
            new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(5) });
        throw new InvalidOperationException("replication factor 2 unexpectedly succeeded");
    }
    catch (CreateTopicsException exception) when (
        exception.Results.Count == 1
        && exception.Results[0].Error.Code == ErrorCode.InvalidReplicationFactor)
    {
        Console.WriteLine("pass   replication factor 2 is rejected");
    }

    Console.WriteLine("PASS   Confluent.Kafka 2.15.0 metadata acceptance");
}
finally
{
    if (process is not null && !process.HasExited)
    {
        process.Kill(entireProcessTree: true);
        await process.WaitForExitAsync();
    }
    process?.Dispose();
}

static Process StartMemKafka(string binary, string workingDirectory)
{
    var startInfo = new ProcessStartInfo(binary)
    {
        WorkingDirectory = workingDirectory,
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        UseShellExecute = false,
    };
    startInfo.ArgumentList.Add("--kafka-listen");
    startInfo.ArgumentList.Add("127.0.0.1:0");
    startInfo.ArgumentList.Add("--schema-registry-listen");
    startInfo.ArgumentList.Add("127.0.0.1:0");

    return Process.Start(startInfo)
        ?? throw new InvalidOperationException("failed to start MemKafka");
}

static async Task<string> ReadBootstrapServers(Process process)
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    while (true)
    {
        var line = await process.StandardOutput.ReadLineAsync(timeout.Token);
        if (line is null)
        {
            var stderr = await process.StandardError.ReadToEndAsync(timeout.Token);
            throw new InvalidOperationException(
                $"MemKafka exited before readiness (exit={process.ExitCode}, stderr={stderr})");
        }
        if (!line.Contains(readyMarker, StringComparison.Ordinal))
        {
            continue;
        }

        var match = Regex.Match(line, @"kafka=(?<address>\S+)");
        if (!match.Success)
        {
            throw new InvalidOperationException($"could not parse readiness line: {line}");
        }
        return match.Groups["address"].Value;
    }
}

static void AssertTopicPartitions(Metadata metadata, string topicName, int expectedPartitions)
{
    var topic = metadata.Topics.SingleOrDefault(topic => topic.Topic == topicName)
        ?? throw new InvalidOperationException($"metadata omitted topic '{topicName}'");
    if (topic.Error.IsError)
    {
        throw new InvalidOperationException(
            $"metadata for '{topicName}' failed: {topic.Error.Code} {topic.Error.Reason}");
    }
    if (topic.Partitions.Count != expectedPartitions)
    {
        throw new InvalidOperationException(
            $"topic '{topicName}' has {topic.Partitions.Count} partitions, expected {expectedPartitions}");
    }
}

static string FindRepositoryRoot()
{
    for (var directory = new DirectoryInfo(Directory.GetCurrentDirectory());
         directory is not null;
         directory = directory.Parent)
    {
        if (File.Exists(Path.Combine(directory.FullName, "Cargo.toml")))
        {
            return directory.FullName;
        }
    }
    throw new DirectoryNotFoundException("could not find repository root containing Cargo.toml");
}
