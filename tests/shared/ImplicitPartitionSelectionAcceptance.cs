using System.Collections.Concurrent;
using System.Diagnostics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

internal static class ImplicitPartitionSelectionAcceptance
{
    public static Task AssertKeyedAsync(
        IAdminClient admin,
        string bootstrapServers,
        string mode,
        bool enableIdempotence,
        bool explicitPartition,
        int iterations)
    {
        return AssertCreatedTopicProduceMode(
            admin,
            bootstrapServers,
            mode,
            enableIdempotence,
            explicitPartition,
            iterations,
            iteration => new Message<string, string>
            {
                Key = $"key-{iteration}",
                Value = $"value-{iteration}",
            });
    }

    public static Task AssertKeylessAsync(
        IAdminClient admin,
        string bootstrapServers,
        string mode,
        bool enableIdempotence,
        bool explicitPartition,
        int iterations)
    {
        return AssertCreatedTopicProduceMode(
            admin,
            bootstrapServers,
            mode,
            enableIdempotence,
            explicitPartition,
            iterations,
            iteration => new Message<Null, string> { Value = $"value-{iteration}" });
    }

    private static async Task AssertCreatedTopicProduceMode<TKey>(
        IAdminClient admin,
        string bootstrapServers,
        string mode,
        bool enableIdempotence,
        bool explicitPartition,
        int iterations,
        Func<int, Message<TKey, string>> createMessage)
    {
        var producerErrors = new ConcurrentQueue<string>();
        using var producer = new ProducerBuilder<TKey, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = enableIdempotence,
            MaxInFlight = 1,
            MessageTimeoutMs = 5_000,
            SocketTimeoutMs = 5_000,
            AllowAutoCreateTopics = false,
        })
            .SetErrorHandler((_, error) =>
            {
                producerErrors.Enqueue($"{error.Code}: {error.Reason}");
            })
            .Build();

        var expectedRecords = new Dictionary<string, string>(iterations);
        for (var iteration = 0; iteration < iterations; iteration++)
        {
            var topic = $"partition-selection-{mode}-{Guid.NewGuid():N}";
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

            var message = createMessage(iteration);
            expectedRecords.Add(topic, message.Value);

            DeliveryResult<TKey, string> delivery;
            try
            {
                delivery = explicitPartition
                    ? await producer.ProduceAsync(
                        new TopicPartition(topic, new Partition(0)),
                        message)
                    : await producer.ProduceAsync(topic, message);
            }
            catch (ProduceException<TKey, string> exception)
            {
                throw new InvalidOperationException(
                    $"{mode} Produce failed at iteration {iteration} for '{topic}': "
                    + $"{exception.Error.Code} {exception.Error.Reason}; "
                    + DescribeTopicMetadata(admin, topic, bootstrapServers, producerErrors),
                    exception);
            }

            if (delivery.Partition != new Partition(0)
                || delivery.Offset != new Offset(0)
                || delivery.Status != PersistenceStatus.Persisted)
            {
                throw new InvalidOperationException(
                    $"{mode} Produce returned {delivery.TopicPartitionOffset} "
                    + $"with status {delivery.Status} at iteration {iteration}, "
                    + $"expected {topic}[0]@0");
            }
        }

        using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"partition-selection-{mode}-{Guid.NewGuid():N}",
            EnableAutoCommit = false,
            AllowAutoCreateTopics = false,
            SocketTimeoutMs = 5_000,
        }).Build();
        consumer.Assign(expectedRecords.Keys.Select(topic =>
            new TopicPartitionOffset(topic, new Partition(0), Offset.Beginning)));

        var remaining = new Dictionary<string, string>(expectedRecords);
        var deadline = Stopwatch.StartNew();
        while (remaining.Count > 0 && deadline.Elapsed < TimeSpan.FromSeconds(10))
        {
            var record = consumer.Consume(TimeSpan.FromMilliseconds(250));
            if (record is null)
            {
                continue;
            }
            if (!remaining.Remove(record.Topic, out var expected)
                || record.Partition != new Partition(0)
                || record.Offset != new Offset(0)
                || record.Message.Value != expected)
            {
                throw new InvalidOperationException(
                    $"{mode} consumed unexpected record {record.TopicPartitionOffset}: "
                    + $"value={record.Message.Value}");
            }
        }
        if (remaining.Count > 0)
        {
            throw new InvalidOperationException(
                $"{mode} did not consume records from: {string.Join(", ", remaining.Keys)}");
        }
        consumer.Close();
    }

    private static string DescribeTopicMetadata(
        IAdminClient admin,
        string topic,
        string bootstrapServers,
        ConcurrentQueue<string> producerErrors)
    {
        string metadataSummary;
        try
        {
            var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(5));
            var topicMetadata = metadata.Topics.SingleOrDefault(candidate => candidate.Topic == topic);
            metadataSummary = topicMetadata is null
                ? "admin metadata omitted the topic"
                : $"admin metadata error={topicMetadata.Error.Code}, "
                    + $"partitions=[{string.Join(',', topicMetadata.Partitions.Select(partition => partition.PartitionId))}]";
        }
        catch (Exception exception)
        {
            metadataSummary =
                $"admin metadata lookup failed: {exception.GetType().Name}: {exception.Message}";
        }

        var errors = producerErrors.IsEmpty
            ? "none"
            : string.Join(" | ", producerErrors.TakeLast(8));
        return $"bootstrap={bootstrapServers}; {metadataSummary}; producer errors={errors}";
    }
}
