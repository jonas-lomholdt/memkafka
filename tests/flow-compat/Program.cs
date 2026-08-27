using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

var repositoryRoot = FindRepositoryRoot();
Process? process = null;
ProcessOutput? processOutput = null;
IConsumer<string, string>? subscriptionConsumer = null;
IConsumer<string, string>? orderedConsumer = null;

try
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
    processOutput = new ProcessOutput(process);
    var endpoints = await processOutput.ReadEndpointsAsync(process);
    var bootstrapServers = endpoints.BootstrapServers;
    var suffix = Guid.NewGuid().ToString("N");
    var topics = new[]
    {
        $"edi-moves-inbound-{suffix}",
        $"rkem-moves-inbound-{suffix}",
        $"comet-itinerary-update-{suffix}",
        $"comet-moves-outbound-{suffix}",
    };

    var assigned = new TaskCompletionSource<IReadOnlyList<TopicPartition>>(
        TaskCreationOptions.RunContinuationsAsynchronously);
    subscriptionConsumer = new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = bootstrapServers,
        GroupId = $"flow-compat-{suffix}",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
        SessionTimeoutMs = 10_000,
        SocketTimeoutMs = 5_000,
    })
        .SetPartitionsAssignedHandler((_, partitions) =>
        {
            assigned.TrySetResult(partitions);
        })
        .Build();
    subscriptionConsumer.Subscribe(topics);
    var assignment = await ConsumeUntilAssignedAsync(subscriptionConsumer, assigned);

    using var admin = new AdminClientBuilder(new AdminClientConfig
    {
        BootstrapServers = bootstrapServers,
        SocketTimeoutMs = 5_000,
        ApiVersionRequestTimeoutMs = 5_000,
    }).Build();
    foreach (var topic in topics)
    {
        AssertTopicPartitions(admin.GetMetadata(topic, TimeSpan.FromSeconds(5)), topic, 2);
    }
    AssertAssignedTopics(assignment, topics);

    var idempotentTopic = $"flow-idempotent-{suffix}";
    await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = idempotentTopic,
                NumPartitions = 1,
                ReplicationFactor = 1,
            },
        ],
        new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(5) });

    using (var producer = new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = bootstrapServers,
        EnableIdempotence = true,
        MessageTimeoutMs = 5_000,
        SocketTimeoutMs = 5_000,
    })
        .Build())
    {
        for (var index = 0; index < 10; index++)
        {
            var delivery = await producer.ProduceAsync(
                new TopicPartition(idempotentTopic, new Partition(0)),
                new Message<string, string>
                {
                    Key = $"key-{index}",
                    Value = $"value-{index}",
                });
            if (delivery.Offset != new Offset(index))
            {
                throw new InvalidOperationException(
                    $"idempotent Produce returned offset {delivery.Offset}, expected {index}");
            }
        }
    }

    orderedConsumer = new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = bootstrapServers,
        GroupId = $"flow-ordered-{suffix}",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false,
        SocketTimeoutMs = 5_000,
    }).Build();
    orderedConsumer.Assign(new TopicPartitionOffset(
        idempotentTopic,
        new Partition(0),
        new Offset(0)));
    AssertOrderedValues(ConsumeExactly(orderedConsumer, 10), idempotentTopic);

    Console.WriteLine("PASS   Confluent.Kafka 2.13.2 forced subscriptions and idempotent produce/consume");
}
finally
{
    CloseConsumer(orderedConsumer);
    CloseConsumer(subscriptionConsumer);
    if (process is not null && !process.HasExited)
    {
        process.Kill(entireProcessTree: true);
        await process.WaitForExitAsync();
    }
    if (processOutput is not null)
    {
        await processOutput.CompleteAsync();
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
    startInfo.ArgumentList.Add("--auto-create-topics");
    startInfo.ArgumentList.Add("true");
    startInfo.ArgumentList.Add("--force-auto-create-topics");
    startInfo.ArgumentList.Add("true");

    return Process.Start(startInfo)
        ?? throw new InvalidOperationException("failed to start MemKafka");
}

static async Task<IReadOnlyList<TopicPartition>> ConsumeUntilAssignedAsync(
    IConsumer<string, string> consumer,
    TaskCompletionSource<IReadOnlyList<TopicPartition>> assigned)
{
    var deadline = Stopwatch.StartNew();
    while (!assigned.Task.IsCompleted && deadline.Elapsed < TimeSpan.FromSeconds(10))
    {
        consumer.Consume(TimeSpan.FromMilliseconds(250));
    }
    if (assigned.Task.IsCompleted)
    {
        return await assigned.Task;
    }
    throw new TimeoutException("consumer did not receive a partition assignment within 10 seconds");
}

static void AssertTopicPartitions(
    Metadata metadata,
    string topicName,
    int expectedPartitions)
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

static void AssertAssignedTopics(IReadOnlyList<TopicPartition> assignment, string[] topics)
{
    var assignedTopics = assignment.Select(topicPartition => topicPartition.Topic).ToHashSet(StringComparer.Ordinal);
    if (!assignedTopics.SetEquals(topics))
    {
        throw new InvalidOperationException(
            $"assigned topics were [{string.Join(',', assignedTopics)}], expected [{string.Join(',', topics)}]");
    }
}

static List<ConsumeResult<string, string>> ConsumeExactly(
    IConsumer<string, string> consumer,
    int count)
{
    var deadline = Stopwatch.StartNew();
    var records = new List<ConsumeResult<string, string>>(count);
    while (records.Count < count && deadline.Elapsed < TimeSpan.FromSeconds(10))
    {
        var record = consumer.Consume(TimeSpan.FromMilliseconds(250));
        if (record is not null)
        {
            records.Add(record);
        }
    }
    if (records.Count != count)
    {
        throw new InvalidOperationException($"consumed {records.Count} records, expected {count}");
    }
    return records;
}

static void AssertOrderedValues(List<ConsumeResult<string, string>> records, string topic)
{
    for (var index = 0; index < records.Count; index++)
    {
        var record = records[index];
        if (record.Topic != topic
            || record.Partition != new Partition(0)
            || record.Offset != new Offset(index)
            || record.Message.Value != $"value-{index}")
        {
            throw new InvalidOperationException(
                $"unexpected record at index {index}: {record.TopicPartitionOffset}, "
                + $"value={record.Message.Value}");
        }
    }
}

static void CloseConsumer(IConsumer<string, string>? consumer)
{
    if (consumer is null)
    {
        return;
    }
    try
    {
        consumer.Close();
    }
    catch (KafkaException)
    {
        // The broker can already be gone while unwinding a failed acceptance run.
    }
    finally
    {
        consumer.Dispose();
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

sealed class ProcessOutput
{
    private const string ReadyMarker = "MemKafka ready kafka=";
    private static readonly Regex AnsiEscapeSequence = new(
        @"\x1B\[[0-?]*[ -/]*[@-~]",
        RegexOptions.Compiled);
    private readonly ConcurrentQueue<string> standardOutput = new();
    private readonly ConcurrentQueue<string> standardError = new();
    private readonly TaskCompletionSource<MemKafkaEndpoints> readiness = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Task outputPump;
    private readonly Task errorPump;

    public ProcessOutput(Process process)
    {
        outputPump = PumpAsync(process.StandardOutput, standardOutput);
        errorPump = PumpAsync(process.StandardError, standardError);
    }

    public async Task<MemKafkaEndpoints> ReadEndpointsAsync(Process process)
    {
        var processExit = process.WaitForExitAsync();
        var completed = await Task.WhenAny(
            readiness.Task,
            processExit,
            Task.Delay(TimeSpan.FromSeconds(10)));
        if (completed == readiness.Task)
        {
            return await readiness.Task;
        }
        if (completed == processExit)
        {
            throw new InvalidOperationException(
                $"MemKafka exited with code {process.ExitCode} before reporting readiness. {Diagnostics()}");
        }
        throw new InvalidOperationException(
            $"MemKafka did not report readiness within 10 seconds. {Diagnostics()}");
    }

    public Task CompleteAsync()
    {
        return Task.WhenAll(outputPump, errorPump);
    }

    private async Task PumpAsync(StreamReader reader, ConcurrentQueue<string> destination)
    {
        while (await reader.ReadLineAsync() is { } line)
        {
            line = AnsiEscapeSequence.Replace(line, string.Empty);
            destination.Enqueue(line);
            if (!line.Contains(ReadyMarker, StringComparison.Ordinal))
            {
                continue;
            }

            var match = Regex.Match(
                line,
                @"kafka=(?<kafka>\S+) schema_registry=(?<schemaRegistry>\S+)");
            if (match.Success)
            {
                readiness.TrySetResult(new MemKafkaEndpoints(
                    match.Groups["kafka"].Value,
                    match.Groups["schemaRegistry"].Value));
            }
            else
            {
                readiness.TrySetException(
                    new InvalidOperationException($"could not parse readiness line: {line}"));
            }
        }
    }

    private string Diagnostics()
    {
        return $"stdout={string.Join(" | ", standardOutput)}; "
            + $"stderr={string.Join(" | ", standardError)}";
    }
}

sealed record MemKafkaEndpoints(string BootstrapServers, string SchemaRegistryUrl);
