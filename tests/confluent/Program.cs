using System.Collections.Concurrent;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.RegularExpressions;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

var repositoryRoot = FindRepositoryRoot();
Process? process = null;
ProcessOutput? processOutput = null;
try
{
    var bootstrapServers = Environment.GetEnvironmentVariable("MEMKAFKA_BOOTSTRAP_SERVERS");
    var schemaRegistryUrl = Environment.GetEnvironmentVariable("MEMKAFKA_SCHEMA_REGISTRY_URL");
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
        processOutput = new ProcessOutput(process);
        var endpoints = await processOutput.ReadEndpointsAsync();
        bootstrapServers = endpoints.BootstrapServers;
        schemaRegistryUrl = endpoints.SchemaRegistryUrl;
    }
    if (string.IsNullOrWhiteSpace(schemaRegistryUrl))
    {
        schemaRegistryUrl = "http://127.0.0.1:8081";
    }
    Console.WriteLine($"ready  kafka={bootstrapServers} schema_registry={schemaRegistryUrl}");

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

    await AssertCooperativeMultiMemberLifecycle(admin, bootstrapServers);
    Console.WriteLine("pass   cooperative A/B/C joins, graceful leave, and session expiry stay disjoint");
    if (processOutput is not null)
    {
        await processOutput.AssertCooperativeSelectionLogAsync();
        Console.WriteLine("pass   cooperative selection is visible in the broker info log");
    }

    await AssertSchemaRegistryAndAvro(admin, bootstrapServers, schemaRegistryUrl);
    Console.WriteLine("pass   Schema Registry IDs, versions, errors, and real Avro publish/consume round-trip");

    Console.WriteLine("PASS   Confluent.Kafka and Schema Registry Avro 2.15.0 black-box acceptance");
}
finally
{
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

    return Process.Start(startInfo)
        ?? throw new InvalidOperationException("failed to start MemKafka");
}

static void AssertTopicPartitions(
    Confluent.Kafka.Metadata metadata,
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

static async Task AssertSchemaRegistryAndAvro(
    IAdminClient admin,
    string bootstrapServers,
    string schemaRegistryUrl)
{
    var topic = $"avro-{Guid.NewGuid():N}";
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

    const string schemaText = """
        {
          "type": "record",
          "name": "OrderCreated",
          "namespace": "MemKafka.Acceptance",
          "fields": [
            { "name": "orderId", "type": "string" },
            { "name": "sequence", "type": "long" }
          ]
        }
        """;
    const string secondSchemaText = """
        {
          "type": "record",
          "name": "OrderCancelled",
          "namespace": "MemKafka.Acceptance",
          "fields": [
            { "name": "orderId", "type": "string" }
          ]
        }
        """;
    var avroSchema = (RecordSchema)RecordSchema.Parse(schemaText);
    var record = new GenericRecord(avroSchema);
    record.Add("orderId", "order-42");
    record.Add("sequence", 7L);
    var subject = $"{topic}-value";

    using var registry = new CachedSchemaRegistryClient(
        new SchemaRegistryConfig { Url = schemaRegistryUrl });
    DeliveryResult<Null, GenericRecord> delivery;
    using (var producer = new ProducerBuilder<Null, GenericRecord>(new ProducerConfig
    {
        BootstrapServers = bootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = false,
        MaxInFlight = 1,
        MessageTimeoutMs = 5_000,
        SocketTimeoutMs = 5_000,
        AllowAutoCreateTopics = false,
    })
        .SetValueSerializer(new AvroSerializer<GenericRecord>(registry))
        .Build())
    {
        delivery = await producer.ProduceAsync(
            new TopicPartition(topic, new Partition(0)),
            new Message<Null, GenericRecord> { Value = record });
    }
    if (delivery.Offset != new Offset(0))
    {
        throw new InvalidOperationException($"Avro delivery returned {delivery.Offset}, expected 0");
    }

    var subjects = await registry.GetAllSubjectsAsync();
    if (!subjects.Contains(subject, StringComparer.Ordinal))
    {
        throw new InvalidOperationException($"subjects omitted auto-registered '{subject}'");
    }
    var versions = await registry.GetSubjectVersionsAsync(subject);
    if (!versions.SequenceEqual([1]))
    {
        throw new InvalidOperationException(
            $"auto-registered versions were [{string.Join(',', versions)}], expected [1]");
    }
    var latest = await registry.GetLatestSchemaAsync(subject);
    if (latest.Id <= 0 || latest.Version != 1 || latest.Subject != subject)
    {
        throw new InvalidOperationException(
            $"unexpected latest schema: subject={latest.Subject} version={latest.Version} id={latest.Id}");
    }
    var byId = await registry.GetSchemaAsync(latest.Id);
    var byVersion = await registry.GetRegisteredSchemaAsync(subject, 1);
    if (byId.SchemaString != latest.Schema.SchemaString
        || byVersion.Id != latest.Id
        || byVersion.Schema.SchemaString != latest.Schema.SchemaString)
    {
        throw new InvalidOperationException("schema fetch by ID or subject version changed the schema");
    }

    using (var freshRegistry = new CachedSchemaRegistryClient(
        new SchemaRegistryConfig { Url = schemaRegistryUrl }))
    {
        var duplicateId = await freshRegistry.RegisterSchemaAsync(subject, latest.Schema);
        if (duplicateId != latest.Id
            || !(await freshRegistry.GetSubjectVersionsAsync(subject)).SequenceEqual([1]))
        {
            throw new InvalidOperationException("identical schema registration was not deduplicated");
        }

        var secondId = await freshRegistry.RegisterSchemaAsync(
            subject,
            new Confluent.SchemaRegistry.Schema(secondSchemaText, SchemaType.Avro));
        if (secondId <= latest.Id
            || !(await freshRegistry.GetSubjectVersionsAsync(subject)).SequenceEqual([1, 2]))
        {
            throw new InvalidOperationException("distinct schema did not allocate the next ID and version");
        }

        var otherSubject = $"other-{Guid.NewGuid():N}-value";
        var reusedId = await freshRegistry.RegisterSchemaAsync(otherSubject, latest.Schema);
        if (reusedId != latest.Id
            || !(await freshRegistry.GetSubjectVersionsAsync(otherSubject)).SequenceEqual([1]))
        {
            throw new InvalidOperationException("exact schema text did not reuse its global ID across subjects");
        }
    }

    using (var wireConsumer = new ConsumerBuilder<Ignore, byte[]>(new ConsumerConfig
    {
        BootstrapServers = bootstrapServers,
        GroupId = $"avro-wire-{Guid.NewGuid():N}",
        EnableAutoCommit = false,
        AllowAutoCreateTopics = false,
        SocketTimeoutMs = 5_000,
    }).Build())
    {
        wireConsumer.Assign(new TopicPartitionOffset(topic, 0, Offset.Beginning));
        var wireRecord = wireConsumer.Consume(TimeSpan.FromSeconds(5));
        var payload = wireRecord?.Message.Value;
        if (payload is null || payload.Length < 5 || payload[0] != 0)
        {
            throw new InvalidOperationException("Avro record omitted the Confluent wire-format header");
        }
        var wireSchemaId = BinaryPrimitives.ReadInt32BigEndian(payload.AsSpan(1, 4));
        if (wireSchemaId != latest.Id)
        {
            throw new InvalidOperationException(
                $"wire schema ID was {wireSchemaId}, expected {latest.Id}");
        }
    }

    using var deserializerRegistry = new CachedSchemaRegistryClient(
        new SchemaRegistryConfig { Url = schemaRegistryUrl });
    using (var avroConsumer = new ConsumerBuilder<Ignore, GenericRecord>(new ConsumerConfig
    {
        BootstrapServers = bootstrapServers,
        GroupId = $"avro-value-{Guid.NewGuid():N}",
        EnableAutoCommit = false,
        AllowAutoCreateTopics = false,
        SocketTimeoutMs = 5_000,
    })
        .SetValueDeserializer(
            new AvroDeserializer<GenericRecord>(deserializerRegistry).AsSyncOverAsync())
        .Build())
    {
        avroConsumer.Assign(new TopicPartitionOffset(topic, 0, Offset.Beginning));
        var consumed = avroConsumer.Consume(TimeSpan.FromSeconds(5));
        if (consumed?.Message.Value["orderId"]?.ToString() != "order-42"
            || !Equals(consumed.Message.Value["sequence"], 7L))
        {
            throw new InvalidOperationException("real Avro deserializer did not recover the record");
        }
    }

    try
    {
        await registry.GetSchemaAsync(int.MaxValue);
        throw new InvalidOperationException("missing schema ID unexpectedly succeeded");
    }
    catch (SchemaRegistryException exception) when (exception.ErrorCode == 40403)
    {
    }
    try
    {
        await registry.GetSubjectVersionsAsync($"missing-{Guid.NewGuid():N}");
        throw new InvalidOperationException("missing subject unexpectedly succeeded");
    }
    catch (SchemaRegistryException exception) when (exception.ErrorCode == 40401)
    {
    }
    try
    {
        await registry.GetRegisteredSchemaAsync(subject, int.MaxValue);
        throw new InvalidOperationException("missing subject version unexpectedly succeeded");
    }
    catch (SchemaRegistryException exception) when (exception.ErrorCode == 40402)
    {
    }

    using var http = new HttpClient
    {
        BaseAddress = new Uri(schemaRegistryUrl.TrimEnd('/') + "/"),
        Timeout = TimeSpan.FromSeconds(5),
    };
    var unsupported = await http.PostAsJsonAsync(
        $"subjects/{Uri.EscapeDataString(subject)}/versions",
        new { schema = schemaText, schemaType = "PROTOBUF" });
    var error = JsonDocument.Parse(await unsupported.Content.ReadAsStringAsync()).RootElement;
    if (unsupported.StatusCode != HttpStatusCode.UnprocessableEntity
        || error.GetProperty("error_code").GetInt32() != 42201)
    {
        throw new InvalidOperationException(
            $"unsupported schema type returned HTTP {(int)unsupported.StatusCode}: {error}");
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

    consumer.Seek(new TopicPartitionOffset(partition, new Offset(5)));
    var sought = consumer.Consume(TimeSpan.FromSeconds(5));
    if (sought is null || sought.TopicPartitionOffset != new TopicPartitionOffset(partition, 5))
    {
        throw new InvalidOperationException(
            $"seek to {partition}@5 returned {sought?.TopicPartitionOffset}");
    }
    consumer.Close();

    using var latest = new ConsumerBuilder<string, string>(new ConsumerConfig
    {
        BootstrapServers = bootstrapServers,
        GroupId = $"latest-{Guid.NewGuid():N}",
        EnableAutoCommit = false,
        AutoOffsetReset = AutoOffsetReset.Latest,
        PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        AllowAutoCreateTopics = false,
        SocketTimeoutMs = 5_000,
        SessionTimeoutMs = 6_000,
    }).Build();
    latest.Subscribe(topic);
    if (latest.Consume(TimeSpan.FromMilliseconds(750)) is not null)
    {
        throw new InvalidOperationException("latest reset returned an existing record");
    }
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
        await producer.ProduceAsync(
            partition,
            new Message<string, string> { Key = "key-10", Value = "message-10" });
    }
    var latestRecord = latest.Consume(TimeSpan.FromSeconds(5));
    if (latestRecord is null || latestRecord.TopicPartitionOffset != new TopicPartitionOffset(partition, 10))
    {
        throw new InvalidOperationException(
            $"latest reset returned {latestRecord?.TopicPartitionOffset}, expected {partition}@10");
    }
    latest.Close();
}

static async Task AssertConsumerGroupOffsets(
    IAdminClient admin,
    string bootstrapServers)
{
    await AssertAutoCommitResume(admin, bootstrapServers);
    await AssertManualCommitResume(admin, bootstrapServers);
    await AssertNoCommitRedelivery(admin, bootstrapServers);
    await AssertSeparateGroupsCommitIndependently(admin, bootstrapServers);
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

static async Task AssertSeparateGroupsCommitIndependently(
    IAdminClient admin,
    string bootstrapServers)
{
    var topic = await CreateGroupTopic(admin, bootstrapServers, "separate-groups");
    var firstGroup = $"first-{Guid.NewGuid():N}";
    var secondGroup = $"second-{Guid.NewGuid():N}";

    using (var first = BuildGroupConsumer(bootstrapServers, firstGroup, false))
    {
        first.Subscribe(topic);
        var record = first.Consume(TimeSpan.FromSeconds(5));
        AssertGroupRecord(record, topic, 0);
        first.Commit(record);
        first.Close();
    }
    using (var second = BuildGroupConsumer(bootstrapServers, secondGroup, false))
    {
        second.Subscribe(topic);
        AssertGroupRecord(second.Consume(TimeSpan.FromSeconds(5)), topic, 0);
        var record = second.Consume(TimeSpan.FromSeconds(5));
        AssertGroupRecord(record, topic, 1);
        second.Commit(record);
        second.Close();
    }

    using var restartedFirst = BuildGroupConsumer(bootstrapServers, firstGroup, false);
    restartedFirst.Subscribe(topic);
    AssertGroupRecord(restartedFirst.Consume(TimeSpan.FromSeconds(5)), topic, 1);
    restartedFirst.Close();

    using var restartedSecond = BuildGroupConsumer(bootstrapServers, secondGroup, false);
    restartedSecond.Subscribe(topic);
    AssertGroupRecord(restartedSecond.Consume(TimeSpan.FromSeconds(5)), topic, 2);
    restartedSecond.Close();
}

static async Task AssertCooperativeMultiMemberLifecycle(
    IAdminClient admin,
    string bootstrapServers)
{
    var topic = $"cooperative-{Guid.NewGuid():N}";
    await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = 6,
                ReplicationFactor = 1,
            },
        ],
        new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(5) });
    var groupId = $"cooperative-{Guid.NewGuid():N}";
    var observer = new CooperativeAssignmentObserver();
    var consumers = new List<CooperativeConsumerRunner>();
    try
    {
        consumers.Add(
            new CooperativeConsumerRunner(
                bootstrapServers,
                groupId,
                topic,
                "consumer-a",
                observer));
        consumers.Add(
            new CooperativeConsumerRunner(
                bootstrapServers,
                groupId,
                topic,
                "consumer-b",
                observer));
        await WaitForCooperativeCoverage(consumers, 6, TimeSpan.FromSeconds(10));
        var beforeAddingThirdMember = SnapshotAssignments(consumers);

        consumers.Add(
            new CooperativeConsumerRunner(
                bootstrapServers,
                groupId,
                topic,
                "consumer-c",
                observer));
        await WaitForCooperativeCoverage(consumers, 6, TimeSpan.FromSeconds(10));
        AssertMinimalMovementForThirdMember(beforeAddingThirdMember, consumers, "consumer-c", 2);

        var graceful = consumers[^1];
        consumers.RemoveAt(consumers.Count - 1);
        await graceful.StopGracefullyAsync();
        await WaitForCooperativeCoverage(consumers, 6, TimeSpan.FromSeconds(10));

        var crashed = consumers[^1];
        consumers.RemoveAt(consumers.Count - 1);
        var survivorAssignments = SnapshotAssignments(consumers);
        await crashed.StopUngracefullyAsync();
        var noRedistributionWindow = TimeSpan.FromMilliseconds(
            CooperativeConsumerRunner.SessionTimeoutMilliseconds
            - (2 * CooperativeConsumerRunner.HeartbeatIntervalMilliseconds));
        await AssertAssignmentsUnchanged(consumers, survivorAssignments, noRedistributionWindow);
        await WaitForCooperativeCoverage(consumers, 6, TimeSpan.FromSeconds(10));
    }
    finally
    {
        foreach (var consumer in consumers)
        {
            await consumer.StopGracefullyAsync();
        }
    }
}

static async Task WaitForCooperativeCoverage(
    IReadOnlyList<CooperativeConsumerRunner> consumers,
    int partitionCount,
    TimeSpan timeout)
{
    var deadline = Stopwatch.StartNew();
    Stopwatch? stable = null;
    while (deadline.Elapsed < timeout)
    {
        foreach (var consumer in consumers)
        {
            consumer.ThrowIfFailed();
        }
        var assignments = consumers.Select(consumer => consumer.Assignment()).ToList();
        var union = assignments.SelectMany(partitions => partitions).ToHashSet();
        var isComplete = assignments.All(partitions => partitions.Count > 0)
            && union.SetEquals(Enumerable.Range(0, partitionCount))
            && assignments.Sum(partitions => partitions.Count) == partitionCount;
        if (isComplete)
        {
            stable ??= Stopwatch.StartNew();
            if (stable.Elapsed >= TimeSpan.FromMilliseconds(500))
            {
                return;
            }
        }
        else
        {
            stable = null;
        }
        await Task.Delay(25);
    }

    var observed = string.Join(
        "; ",
        consumers.Select(consumer => $"{consumer.Name}=[{string.Join(',', consumer.Assignment())}]"));
    throw new InvalidOperationException(
        $"cooperative assignment did not stabilize over {partitionCount} partitions: {observed}");
}

static Dictionary<string, AssignmentSnapshot> SnapshotAssignments(
    IReadOnlyList<CooperativeConsumerRunner> consumers)
{
    return consumers.ToDictionary(consumer => consumer.Name, consumer => consumer.Snapshot());
}

static void AssertMinimalMovementForThirdMember(
    IReadOnlyDictionary<string, AssignmentSnapshot> before,
    IReadOnlyList<CooperativeConsumerRunner> consumers,
    string addedMember,
    int expectedMovement)
{
    var after = SnapshotAssignments(consumers);
    var moved = before.Sum(entry =>
        entry.Value.Partitions.Count(partition => !after[entry.Key].Partitions.Contains(partition)));
    if (moved != expectedMovement
        || after[addedMember].Partitions.Count != expectedMovement)
    {
        throw new InvalidOperationException(
            $"adding {addedMember} moved {moved} existing partitions and assigned "
            + $"{after[addedMember].Partitions.Count}; expected {expectedMovement} of each");
    }
}

static async Task AssertAssignmentsUnchanged(
    IReadOnlyList<CooperativeConsumerRunner> consumers,
    IReadOnlyDictionary<string, AssignmentSnapshot> expected,
    TimeSpan duration)
{
    var deadline = Stopwatch.StartNew();
    while (deadline.Elapsed < duration)
    {
        foreach (var consumer in consumers)
        {
            consumer.ThrowIfFailed();
            var actual = consumer.Snapshot();
            var expectedAssignment = expected[consumer.Name];
            if (actual.Revision != expectedAssignment.Revision
                || !actual.Partitions.SetEquals(expectedAssignment.Partitions))
            {
                throw new InvalidOperationException(
                    $"{consumer.Name} changed assignment before the session-expiry window: "
                    + $"[{string.Join(',', actual.Partitions)}]");
            }
        }
        await Task.Delay(25);
    }
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

sealed class CooperativeConsumerRunner
{
    public const int SessionTimeoutMilliseconds = 3_000;
    public const int HeartbeatIntervalMilliseconds = 250;
    private readonly IConsumer<string, string> consumer;
    private readonly CooperativeAssignmentObserver observer;
    private readonly Task pollTask;
    private volatile bool closeGracefully = true;
    private volatile bool stop;
    private Exception? failure;

    public CooperativeConsumerRunner(
        string bootstrapServers,
        string groupId,
        string topic,
        string name,
        CooperativeAssignmentObserver observer)
    {
        Name = name;
        this.observer = observer;
        observer.Register(name);
        consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            ClientId = name,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            AllowAutoCreateTopics = false,
            SocketTimeoutMs = 5_000,
            SessionTimeoutMs = SessionTimeoutMilliseconds,
            HeartbeatIntervalMs = HeartbeatIntervalMilliseconds,
        })
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                observer.Assigned(Name, partitions.Select(partition => partition.Partition.Value));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                observer.Revoked(Name, partitions.Select(partition => partition.Partition.Value));
            })
            .SetPartitionsLostHandler((_, partitions) =>
            {
                observer.Revoked(Name, partitions.Select(partition => partition.Partition.Value));
            })
            .Build();
        consumer.Subscribe(topic);
        pollTask = Task.Run(Poll);
    }

    public string Name { get; }

    public HashSet<int> Assignment()
    {
        return Snapshot().Partitions;
    }

    public AssignmentSnapshot Snapshot()
    {
        return observer.Snapshot(Name);
    }

    public void ThrowIfFailed()
    {
        if (failure is not null)
        {
            throw new InvalidOperationException($"{Name} poll loop failed", failure);
        }
        observer.ThrowIfOverlapping();
    }

    public async Task StopGracefullyAsync()
    {
        stop = true;
        await pollTask;
        ThrowIfFailed();
    }

    public async Task StopUngracefullyAsync()
    {
        closeGracefully = false;
        stop = true;
        await pollTask;
        ThrowIfFailed();
    }

    private void Poll()
    {
        try
        {
            while (!stop)
            {
                consumer.Consume(TimeSpan.FromMilliseconds(100));
            }
            if (closeGracefully)
            {
                consumer.Close();
            }
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        finally
        {
            consumer.Dispose();
            observer.Stopped(Name);
        }
    }
}

sealed class ProcessOutput
{
    private const string ReadyMarker = "MemKafka ready kafka=";
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

    public async Task<MemKafkaEndpoints> ReadEndpointsAsync()
    {
        try
        {
            return await readiness.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (TimeoutException exception)
        {
            throw new InvalidOperationException(
                "MemKafka did not report readiness within 10 seconds. " + Diagnostics(),
                exception);
        }
    }

    public async Task AssertCooperativeSelectionLogAsync()
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < TimeSpan.FromSeconds(5))
        {
            if (Lines().Any(line =>
                line.Contains("Using cooperative incremental rebalancing", StringComparison.Ordinal)
                && line.Contains("protocol=\"cooperative-sticky\"", StringComparison.Ordinal)
                && line.Contains("rebalance=\"cooperative\"", StringComparison.Ordinal)
                && line.Contains("members=3", StringComparison.Ordinal)))
            {
                return;
            }
            await Task.Delay(25);
        }

        var cooperativeLines = Lines()
            .Where(line => line.Contains("cooperative", StringComparison.OrdinalIgnoreCase));
        throw new InvalidOperationException(
            "broker logs omitted the cooperative-sticky selection event for three members. "
            + $"Observed cooperative lines: {string.Join(" | ", cooperativeLines)}");
    }

    public Task CompleteAsync()
    {
        return Task.WhenAll(outputPump, errorPump);
    }

    private async Task PumpAsync(
        StreamReader reader,
        ConcurrentQueue<string> destination)
    {
        while (await reader.ReadLineAsync() is { } line)
        {
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

    private IEnumerable<string> Lines()
    {
        return standardOutput.Concat(standardError);
    }

    private string Diagnostics()
    {
        return $"stdout={string.Join(" | ", standardOutput)}; "
            + $"stderr={string.Join(" | ", standardError)}";
    }
}

sealed record MemKafkaEndpoints(string BootstrapServers, string SchemaRegistryUrl);

sealed class CooperativeAssignmentObserver
{
    private readonly object gate = new();
    private readonly Dictionary<string, HashSet<int>> assignments = [];
    private readonly Dictionary<string, long> revisions = [];
    private Exception? overlap;

    public void Register(string member)
    {
        lock (gate)
        {
            assignments.Add(member, []);
            revisions.Add(member, 0);
        }
    }

    public void Assigned(string member, IEnumerable<int> partitions)
    {
        lock (gate)
        {
            var changed = false;
            foreach (var partition in partitions)
            {
                var priorOwner = assignments.FirstOrDefault(entry =>
                    entry.Key != member && entry.Value.Contains(partition));
                if (priorOwner.Key is not null)
                {
                    overlap ??= new InvalidOperationException(
                        $"partition {partition} moved from {priorOwner.Key} to {member} before revocation");
                }
                changed |= assignments[member].Add(partition);
            }
            if (changed)
            {
                revisions[member]++;
            }
        }
    }

    public void Revoked(string member, IEnumerable<int> partitions)
    {
        lock (gate)
        {
            var changed = false;
            foreach (var partition in partitions)
            {
                changed |= assignments[member].Remove(partition);
            }
            if (changed)
            {
                revisions[member]++;
            }
        }
    }

    public void Stopped(string member)
    {
        lock (gate)
        {
            if (assignments[member].Count > 0)
            {
                assignments[member].Clear();
                revisions[member]++;
            }
        }
    }

    public AssignmentSnapshot Snapshot(string member)
    {
        lock (gate)
        {
            return new AssignmentSnapshot([.. assignments[member]], revisions[member]);
        }
    }

    public void ThrowIfOverlapping()
    {
        lock (gate)
        {
            if (overlap is not null)
            {
                throw overlap;
            }
        }
    }
}

sealed record AssignmentSnapshot(HashSet<int> Partitions, long Revision);
