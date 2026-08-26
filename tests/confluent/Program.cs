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

    var deliveryTopic = $"delivery-{Guid.NewGuid():N}";
    await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = deliveryTopic,
                NumPartitions = 1,
                ReplicationFactor = 1,
            },
        ],
        new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(5) });
    await AssertOrderedDeliveryTwice(bootstrapServers, deliveryTopic);
    Console.WriteLine("pass   publish/consume is ordered and uncommitted records can be read again");

    await AssertConsumerGroupOffsets(admin, bootstrapServers);
    Console.WriteLine("pass   group auto/manual commits resume and no-commit restarts redeliver");

    Console.WriteLine("PASS   Confluent.Kafka 2.15.0 black-box acceptance");
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

static async Task AssertOrderedDeliveryTwice(string bootstrapServers, string topic)
{
    var partition = new TopicPartition(topic, new Partition(0));
    using (var producer = new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = bootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = false,
        MaxInFlight = 1,
        MessageTimeoutMs = 5_000,
        SocketTimeoutMs = 5_000,
        AllowAutoCreateTopics = false,
    }).Build())
    {
        for (var index = 0; index < 10; index++)
        {
            var delivery = await producer.ProduceAsync(
                partition,
                new Message<string, string>
                {
                    Key = $"key-{index}",
                    Value = $"message-{index}",
                });
            if (delivery.Offset != new Offset(index))
            {
                throw new InvalidOperationException(
                    $"Produce returned offset {delivery.Offset}, expected {index}");
            }
        }
    }

    using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = bootstrapServers,
        GroupId = $"direct-{Guid.NewGuid():N}",
        EnableAutoCommit = false,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        AllowAutoCreateTopics = false,
        SocketTimeoutMs = 5_000,
    }).Build();

    consumer.Assign(new TopicPartitionOffset(partition, Offset.Beginning));
    AssertSequence(ConsumeExactly(consumer, 10), topic);

    consumer.Assign(new TopicPartitionOffset(partition, new Offset(0)));
    AssertSequence(ConsumeExactly(consumer, 10), topic);
}

static async Task AssertConsumerGroupOffsets(
    IAdminClient admin,
    string bootstrapServers)
{
    await AssertAutoCommitResume(admin, bootstrapServers);
    await AssertManualCommitResume(admin, bootstrapServers);
    await AssertNoCommitRedelivery(admin, bootstrapServers);
}

static async Task AssertAutoCommitResume(IAdminClient admin, string bootstrapServers)
{
    var topic = await CreateGroupTopic(admin, bootstrapServers, "auto-commit");
    var groupId = $"auto-{Guid.NewGuid():N}";

    using (var consumer = BuildGroupConsumer(bootstrapServers, groupId, true, 100))
    {
        consumer.Subscribe(topic);
        AssertGroupRecord(consumer.Consume(TimeSpan.FromSeconds(5)), topic, 0);
        AssertGroupRecord(consumer.Consume(TimeSpan.FromSeconds(5)), topic, 1);
        await WaitForCommittedOffset(consumer, topic, 2);
        consumer.Close();
    }

    using var restarted = BuildGroupConsumer(bootstrapServers, groupId, false);
    restarted.Subscribe(topic);
    AssertGroupRecord(restarted.Consume(TimeSpan.FromSeconds(5)), topic, 2);
    restarted.Close();
}

static async Task WaitForCommittedOffset(
    IConsumer<string, string> consumer,
    string topic,
    long expectedOffset)
{
    var partition = new TopicPartition(topic, new Partition(0));
    var deadline = Stopwatch.StartNew();
    while (deadline.Elapsed < TimeSpan.FromSeconds(5))
    {
        var committed = consumer.Committed([partition], TimeSpan.FromSeconds(1)).Single();
        if (committed.Offset == new Offset(expectedOffset))
        {
            return;
        }
        await Task.Delay(25);
    }
    throw new InvalidOperationException(
        $"auto commit for {partition} did not reach offset {expectedOffset}");
}

static async Task AssertManualCommitResume(IAdminClient admin, string bootstrapServers)
{
    var topic = await CreateGroupTopic(admin, bootstrapServers, "manual-commit");
    var groupId = $"manual-{Guid.NewGuid():N}";

    using (var consumer = BuildGroupConsumer(bootstrapServers, groupId, false))
    {
        consumer.Subscribe(topic);
        var first = consumer.Consume(TimeSpan.FromSeconds(5));
        AssertGroupRecord(first, topic, 0);
        consumer.Commit(first);
        consumer.Close();
    }

    using var restarted = BuildGroupConsumer(bootstrapServers, groupId, false);
    restarted.Subscribe(topic);
    AssertGroupRecord(restarted.Consume(TimeSpan.FromSeconds(5)), topic, 1);
    restarted.Close();
}

static async Task AssertNoCommitRedelivery(IAdminClient admin, string bootstrapServers)
{
    var topic = await CreateGroupTopic(admin, bootstrapServers, "no-commit");
    var groupId = $"uncommitted-{Guid.NewGuid():N}";

    using (var consumer = BuildGroupConsumer(bootstrapServers, groupId, false))
    {
        consumer.Subscribe(topic);
        AssertGroupRecord(consumer.Consume(TimeSpan.FromSeconds(5)), topic, 0);
        consumer.Close();
    }

    using var restarted = BuildGroupConsumer(bootstrapServers, groupId, false);
    restarted.Subscribe(topic);
    AssertGroupRecord(restarted.Consume(TimeSpan.FromSeconds(5)), topic, 0);
    restarted.Close();
}

static async Task<string> CreateGroupTopic(
    IAdminClient admin,
    string bootstrapServers,
    string prefix)
{
    var topic = $"{prefix}-{Guid.NewGuid():N}";
    await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = 1,
                ReplicationFactor = 1,
            },
        ],
        new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(5) });

    using var producer = new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = bootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = false,
        MaxInFlight = 1,
        MessageTimeoutMs = 5_000,
        SocketTimeoutMs = 5_000,
        AllowAutoCreateTopics = false,
    }).Build();
    for (var index = 0; index < 4; index++)
    {
        await producer.ProduceAsync(
            new TopicPartition(topic, new Partition(0)),
            new Message<string, string>
            {
                Key = $"group-key-{index}",
                Value = $"group-message-{index}",
            });
    }
    return topic;
}

static IConsumer<string, string> BuildGroupConsumer(
    string bootstrapServers,
    string groupId,
    bool enableAutoCommit,
    int? autoCommitIntervalMs = null)
{
    return new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = bootstrapServers,
        GroupId = groupId,
        EnableAutoCommit = enableAutoCommit,
        AutoCommitIntervalMs = autoCommitIntervalMs,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        AllowAutoCreateTopics = false,
        SocketTimeoutMs = 5_000,
        SessionTimeoutMs = 6_000,
    }).Build();
}

static void AssertGroupRecord(
    ConsumeResult<string, string>? record,
    string topic,
    int expectedOffset)
{
    if (record is null
        || record.Topic != topic
        || record.Partition != new Partition(0)
        || record.Offset != new Offset(expectedOffset)
        || record.Message.Key != $"group-key-{expectedOffset}"
        || record.Message.Value != $"group-message-{expectedOffset}")
    {
        throw new InvalidOperationException(
            $"expected {topic}[0]@{expectedOffset}, received {record?.TopicPartitionOffset}");
    }
}

static List<ConsumeResult<string, string>> ConsumeExactly(
    IConsumer<string, string> consumer,
    int count)
{
    var deadline = Stopwatch.StartNew();
    var records = new List<ConsumeResult<string, string>>(count);
    while (records.Count < count && deadline.Elapsed < TimeSpan.FromSeconds(5))
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

static void AssertSequence(List<ConsumeResult<string, string>> records, string topic)
{
    for (var index = 0; index < records.Count; index++)
    {
        var record = records[index];
        if (record.Topic != topic
            || record.Partition != new Partition(0)
            || record.Offset != new Offset(index)
            || record.Message.Key != $"key-{index}"
            || record.Message.Value != $"message-{index}")
        {
            throw new InvalidOperationException(
                $"unexpected record at index {index}: {record.TopicPartitionOffset}, "
                + $"key={record.Message.Key}, value={record.Message.Value}");
        }
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
