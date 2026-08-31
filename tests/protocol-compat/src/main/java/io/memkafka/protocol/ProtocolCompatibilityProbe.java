package io.memkafka.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseDataJsonConverter;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeConfigsResponseDataJsonConverter;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseDataJsonConverter;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestData.FetchPartition;
import org.apache.kafka.common.message.FetchRequestData.FetchTopic;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseDataJsonConverter;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseDataJsonConverter;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.HeartbeatResponseDataJsonConverter;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseDataJsonConverter;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.JoinGroupResponseDataJsonConverter;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseDataJsonConverter;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListGroupsResponseDataJsonConverter;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseDataJsonConverter;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseDataJsonConverter;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseDataJsonConverter;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestGroup;
import org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopics;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseDataJsonConverter;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceDataCollection;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseDataJsonConverter;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData.SyncGroupRequestAssignment;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseDataJsonConverter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SyncGroupRequest;

public final class ProtocolCompatibilityProbe {
    static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 5_000;
    private static final int API_VERSIONS_CORRELATION_ID = 1234;
    private static final int FIRST_TYPED_CORRELATION_ID = 10_000;
    private static final int EXPECTED_TYPED_CASES = 28;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String CLIENT_ID = "memkafka-protocol-compatibility";
    private static final String ORACLE_ERROR_MESSAGE = "MemKafka compatibility oracle";
    private static final List<String> EXPECTED_CASE_KEYS = List.of(
            "0:6", "0:8", "1:5", "2:2", "2:4", "3:3", "3:10",
            "8:6", "8:8", "9:4", "9:6", "10:1", "10:3", "11:4", "11:6",
            "12:2", "12:4", "13:0", "13:4", "14:2", "14:4", "15:1",
            "16:1", "18:2", "19:3", "19:7", "22:1", "32:2");

    private ProtocolCompatibilityProbe() {}

    public static void main(String[] arguments) {
        try {
            var command = parseCommandLine(arguments);
            if (command.subcommand() == Subcommand.TYPED_ERRORS) {
                runTypedErrors(command);
            } else {
                runApiVersions(command);
            }
        } catch (Exception failure) {
            System.err.println("protocol compatibility probe failed: " + failure.getMessage());
            System.exit(2);
        }
    }

    static CommandLine parseCommandLine(String[] arguments) {
        if (arguments.length == 0) {
            throw new IllegalArgumentException("expected typed-errors or api-versions");
        }
        var subcommand = switch (arguments[0]) {
            case "typed-errors" -> Subcommand.TYPED_ERRORS;
            case "api-versions" -> Subcommand.API_VERSIONS;
            default -> throw new IllegalArgumentException("unknown subcommand: " + arguments[0]);
        };
        if ((arguments.length - 1) % 2 != 0) {
            throw new IllegalArgumentException("arguments must be --name value pairs");
        }

        var values = new LinkedHashMap<String, String>();
        for (var index = 1; index < arguments.length; index += 2) {
            var name = arguments[index];
            if (!name.startsWith("--")) {
                throw new IllegalArgumentException("expected named argument, got: " + name);
            }
            if (values.putIfAbsent(name, arguments[index + 1]) != null) {
                throw new IllegalArgumentException("duplicate argument: " + name);
            }
        }
        var allowed = subcommand == Subcommand.TYPED_ERRORS
                ? Set.of("--bootstrap-server", "--output")
                : Set.of("--bootstrap-server", "--version", "--output");
        for (var name : values.keySet()) {
            if (!allowed.contains(name)) {
                throw new IllegalArgumentException("unknown argument: " + name);
            }
        }
        if (!values.keySet().equals(allowed)) {
            var missing = new HashSet<>(allowed);
            missing.removeAll(values.keySet());
            throw new IllegalArgumentException("missing required argument(s): " + missing);
        }

        var bootstrap = parseHostPort(values.get("--bootstrap-server"));
        var output = Path.of(values.get("--output")).toAbsolutePath().normalize();
        var parent = output.getParent();
        if (parent == null || !Files.isDirectory(parent)) {
            throw new IllegalArgumentException("output parent is not a directory: " + output);
        }
        if (Files.exists(output) && !Files.isRegularFile(output)) {
            throw new IllegalArgumentException("output is not a file: " + output);
        }

        Short version = null;
        if (subcommand == Subcommand.API_VERSIONS) {
            try {
                version = Short.valueOf(values.get("--version"));
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException("version must be an i16", failure);
            }
        }
        return new CommandLine(subcommand, bootstrap, version, output);
    }

    private static HostPort parseHostPort(String value) {
        if (value == null || value.isBlank() || !value.equals(value.trim())) {
            throw new IllegalArgumentException("bootstrap server must be host:port");
        }
        String host;
        String portText;
        if (value.startsWith("[")) {
            var close = value.indexOf(']');
            if (close <= 1 || close + 2 >= value.length() || value.charAt(close + 1) != ':') {
                throw new IllegalArgumentException("bootstrap server must be [ipv6]:port");
            }
            host = value.substring(1, close);
            portText = value.substring(close + 2);
        } else {
            var colon = value.lastIndexOf(':');
            if (colon <= 0 || colon == value.length() - 1 || value.indexOf(':') != colon) {
                throw new IllegalArgumentException("bootstrap server must be host:port");
            }
            host = value.substring(0, colon);
            portText = value.substring(colon + 1);
        }
        try {
            var port = Integer.parseInt(portText);
            if (port < 1 || port > 65_535) {
                throw new IllegalArgumentException("bootstrap port must be between 1 and 65535");
            }
            return new HostPort(host, port);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("bootstrap port must be an integer", failure);
        }
    }

    static List<TypedErrorCase> typedErrorCases() {
        var cases = new ArrayList<TypedErrorCase>(EXPECTED_TYPED_CASES);
        for (var index = 0; index < EXPECTED_CASE_KEYS.size(); index++) {
            var parts = EXPECTED_CASE_KEYS.get(index).split(":", 2);
            var apiKey = ApiKeys.forId(Short.parseShort(parts[0]));
            var version = Short.parseShort(parts[1]);
            cases.add(new TypedErrorCase(
                    apiKey,
                    version,
                    FIRST_TYPED_CORRELATION_ID + index,
                    buildTypedRequest(apiKey, version)));
        }
        validateTypedErrorCases(cases);
        return List.copyOf(cases);
    }

    static void validateTypedErrorCases(List<TypedErrorCase> cases) {
        var actual = cases.stream()
                .map(testCase -> testCase.apiKey().id + ":" + testCase.version())
                .toList();
        var unique = new HashSet<>(actual);
        if (cases.size() != EXPECTED_TYPED_CASES
                || unique.size() != EXPECTED_TYPED_CASES
                || !actual.equals(EXPECTED_CASE_KEYS)) {
            throw new IllegalStateException(
                    "typed-errors requires exactly 28 unique cases in API-key/version order; got " + actual);
        }
        if (cases.stream().map(TypedErrorCase::apiKey).distinct().count() != 17) {
            throw new IllegalStateException("typed-errors requires exactly 17 unique APIs");
        }
    }

    private static AbstractRequest buildTypedRequest(ApiKeys apiKey, short version) {
        return switch (apiKey) {
            case PRODUCE -> produceRequest(version);
            case FETCH -> fetchRequest(version);
            case LIST_OFFSETS -> listOffsetsRequest(version);
            case METADATA -> metadataRequest(version);
            case OFFSET_COMMIT -> offsetCommitRequest(version);
            case OFFSET_FETCH -> offsetFetchRequest(version);
            case FIND_COORDINATOR -> findCoordinatorRequest(version);
            case JOIN_GROUP -> joinGroupRequest(version);
            case HEARTBEAT -> heartbeatRequest(version);
            case LEAVE_GROUP -> leaveGroupRequest(version);
            case SYNC_GROUP -> syncGroupRequest(version);
            case DESCRIBE_GROUPS -> describeGroupsRequest(version);
            case LIST_GROUPS -> new ListGroupsRequest(new ListGroupsRequestData(), version);
            case API_VERSIONS -> new ApiVersionsRequest(new ApiVersionsRequestData(), version);
            case CREATE_TOPICS -> createTopicsRequest(version);
            case INIT_PRODUCER_ID -> new InitProducerIdRequest.Builder(
                    new InitProducerIdRequestData()
                            .setTransactionalId(null)
                            .setTransactionTimeoutMs(10_000))
                    .build(version);
            case DESCRIBE_CONFIGS -> describeConfigsRequest(version);
            default -> throw new IllegalArgumentException("unexpected typed API: " + apiKey);
        };
    }

    private static AbstractRequest produceRequest(short version) {
        return new ProduceRequest(new ProduceRequestData()
                .setAcks((short) 1)
                .setTimeoutMs(1_234)
                .setTopicData(new TopicProduceDataCollection(List.of(
                        produceTopic("oracle-produce-a", 7, 17),
                        produceTopic("oracle-produce-b", 27, 37)).iterator())), version);
    }

    private static TopicProduceData produceTopic(String name, int first, int second) {
        return new TopicProduceData().setName(name).setPartitionData(List.of(
                new PartitionProduceData().setIndex(first).setRecords(MemoryRecords.EMPTY),
                new PartitionProduceData().setIndex(second).setRecords(MemoryRecords.EMPTY)));
    }

    private static AbstractRequest fetchRequest(short version) {
        return new FetchRequest(new FetchRequestData()
                .setReplicaId(-1)
                .setMaxWaitMs(0)
                .setMinBytes(0)
                .setMaxBytes(4_096)
                .setTopics(List.of(
                        fetchTopic("oracle-fetch-a", 8, 18),
                        fetchTopic("oracle-fetch-b", 28, 38))), version);
    }

    private static FetchTopic fetchTopic(String name, int first, int second) {
        return new FetchTopic().setTopic(name).setPartitions(List.of(
                new FetchPartition()
                        .setPartition(first)
                        .setFetchOffset(456L + first)
                        .setPartitionMaxBytes(2_048),
                new FetchPartition()
                        .setPartition(second)
                        .setFetchOffset(456L + second)
                        .setPartitionMaxBytes(2_048)));
    }

    private static AbstractRequest listOffsetsRequest(short version) {
        var topics = List.of(
                offsetsTopic("oracle-list-offsets-a", 9, 19),
                offsetsTopic("oracle-list-offsets-b", 29, 39));
        return ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(topics)
                .build(version);
    }

    private static ListOffsetsTopic offsetsTopic(String name, int first, int second) {
        return new ListOffsetsTopic().setName(name).setPartitions(List.of(
                new ListOffsetsPartition()
                        .setPartitionIndex(first)
                        .setTimestamp(1_725_000_000_000L + first),
                new ListOffsetsPartition()
                        .setPartitionIndex(second)
                        .setTimestamp(1_725_000_000_000L + second)));
    }

    private static AbstractRequest metadataRequest(short version) {
        return new MetadataRequest(new MetadataRequestData().setTopics(List.of(
                new MetadataRequestTopic().setName("oracle-metadata-a"),
                new MetadataRequestTopic().setName("oracle-metadata-b"))), version);
    }

    private static AbstractRequest offsetCommitRequest(short version) {
        return new OffsetCommitRequest(new OffsetCommitRequestData()
                .setGroupId("oracle-commit-group")
                .setMemberId("oracle-commit-member")
                .setTopics(List.of(
                        commitTopic("oracle-commit-a", 10, 20),
                        commitTopic("oracle-commit-b", 30, 40))), version);
    }

    private static OffsetCommitRequestTopic commitTopic(String name, int first, int second) {
        return new OffsetCommitRequestTopic().setName(name).setPartitions(List.of(
                new OffsetCommitRequestPartition()
                        .setPartitionIndex(first)
                        .setCommittedOffset(900L + first),
                new OffsetCommitRequestPartition()
                        .setPartitionIndex(second)
                        .setCommittedOffset(900L + second)));
    }

    private static AbstractRequest offsetFetchRequest(short version) {
        var group = new OffsetFetchRequestGroup()
                .setGroupId("oracle-offset-fetch-group")
                .setTopics(List.of(
                        offsetFetchTopic("oracle-offset-fetch-a", 11, 21),
                        offsetFetchTopic("oracle-offset-fetch-b", 31, 41)));
        return OffsetFetchRequest.Builder.forTopicNames(
                        new OffsetFetchRequestData().setGroups(List.of(group)), false)
                .build(version);
    }

    private static OffsetFetchRequestTopics offsetFetchTopic(String name, int first, int second) {
        return new OffsetFetchRequestTopics()
                .setName(name)
                .setPartitionIndexes(List.of(first, second));
    }

    private static AbstractRequest findCoordinatorRequest(short version) {
        return new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                .setKey("oracle-coordinator")
                .setKeyType((byte) 0))
                .build(version);
    }

    private static AbstractRequest joinGroupRequest(short version) {
        return new JoinGroupRequest(new JoinGroupRequestData()
                .setGroupId("oracle-join-group")
                .setSessionTimeoutMs(10_000)
                .setRebalanceTimeoutMs(10_000)
                .setMemberId("oracle-join-member")
                .setProtocolType("consumer")
                .setProtocols(new JoinGroupRequestProtocolCollection(List.of(
                        new JoinGroupRequestProtocol().setName("range").setMetadata(new byte[0]),
                        new JoinGroupRequestProtocol().setName("roundrobin").setMetadata(new byte[0])).iterator())), version);
    }

    private static AbstractRequest heartbeatRequest(short version) {
        return new HeartbeatRequest.Builder(new HeartbeatRequestData()
                .setGroupId("oracle-heartbeat-group")
                .setGenerationId(7)
                .setMemberId("oracle-heartbeat-member"))
                .build(version);
    }

    private static AbstractRequest leaveGroupRequest(short version) {
        var members = version <= 2
                ? List.of(new MemberIdentity().setMemberId("oracle-leave-member"))
                : List.of(
                        new MemberIdentity()
                                .setMemberId("oracle-leave-member-a")
                                .setGroupInstanceId("oracle-leave-instance-a"),
                        new MemberIdentity()
                                .setMemberId("oracle-leave-member-b")
                                .setGroupInstanceId("oracle-leave-instance-b"));
        return new LeaveGroupRequest.Builder("oracle-leave-group", members).build(version);
    }

    private static AbstractRequest syncGroupRequest(short version) {
        return new SyncGroupRequest(new SyncGroupRequestData()
                .setGroupId("oracle-sync-group")
                .setGenerationId(8)
                .setMemberId("oracle-sync-member")
                .setAssignments(List.of(
                        new SyncGroupRequestAssignment()
                                .setMemberId("oracle-sync-member-a")
                                .setAssignment(new byte[0]),
                        new SyncGroupRequestAssignment()
                                .setMemberId("oracle-sync-member-b")
                                .setAssignment(new byte[0]))), version);
    }

    private static AbstractRequest describeGroupsRequest(short version) {
        return new DescribeGroupsRequest.Builder(new DescribeGroupsRequestData()
                .setGroups(List.of("oracle-describe-group-a", "oracle-describe-group-b")))
                .build(version);
    }

    private static AbstractRequest createTopicsRequest(short version) {
        return new CreateTopicsRequest(new CreateTopicsRequestData()
                .setTimeoutMs(1_234)
                .setTopics(new CreatableTopicCollection(List.of(
                        new CreatableTopic()
                                .setName("oracle-create-a")
                                .setNumPartitions(5)
                                .setReplicationFactor((short) 1),
                        new CreatableTopic()
                                .setName("oracle-create-b")
                                .setNumPartitions(7)
                                .setReplicationFactor((short) 1)).iterator())), version);
    }

    private static AbstractRequest describeConfigsRequest(short version) {
        return new DescribeConfigsRequest(new DescribeConfigsRequestData().setResources(List.of(
                new DescribeConfigsResource()
                        .setResourceType((byte) 2)
                        .setResourceName("oracle-config-topic"),
                new DescribeConfigsResource()
                        .setResourceType((byte) 4)
                        .setResourceName("oracle-config-broker"))), version);
    }

    private static void runTypedErrors(CommandLine command) throws Exception {
        var cases = typedErrorCases();
        var output = JSON.createObjectNode();
        output.put("schemaVersion", 1);
        output.put("kafkaClientsVersion", "4.3.1");
        output.put("caseCount", cases.size());
        var normalizedCases = output.putArray("cases");

        for (var testCase : cases) {
            var header = new RequestHeader(
                    testCase.apiKey(), testCase.version(), CLIENT_ID, testCase.correlationId());
            var requestBytes = testCase.request().serializeWithHeader(header);
            var expected = testCase.request().getErrorResponse(
                    0, new UnsupportedVersionException(ORACLE_ERROR_MESSAGE));
            if (expected == null) {
                throw new IllegalStateException(testCase.apiKey() + " v" + testCase.version()
                        + " unexpectedly has no typed error response");
            }
            var responseBytes = exchange(command.bootstrapServer(), requestBytes);
            var actualEnvelope = parseTypedResponse(testCase, responseBytes);
            var expectedNormalized = normalizeTypedResponse(
                    testCase, expected, new ResponseHeader(
                            testCase.correlationId(),
                            testCase.apiKey().responseHeaderVersion(testCase.version())));
            var actualNormalized = normalizeTypedResponse(
                    testCase, actualEnvelope.response(), actualEnvelope.header());
            requireNormalizedMatch(
                    testCase.apiKey(), testCase.version(), expectedNormalized, actualNormalized);
            normalizedCases.add(actualNormalized);
        }
        writeJson(command.output(), output);
    }

    private static ParsedResponse parseTypedResponse(TypedErrorCase testCase, byte[] bytes) {
        var buffer = ByteBuffer.wrap(bytes);
        var headerVersion = testCase.apiKey().responseHeaderVersion(testCase.version());
        var header = ResponseHeader.parse(buffer, headerVersion);
        if (header.correlationId() != testCase.correlationId()) {
            throw new IllegalStateException(testCase.apiKey() + " v" + testCase.version()
                    + " wrong correlation ID: " + header.correlationId());
        }
        var response = AbstractResponse.parseResponse(
                testCase.apiKey(), new ByteBufferAccessor(buffer), testCase.version());
        if (buffer.hasRemaining()) {
            throw new IllegalStateException(testCase.apiKey() + " v" + testCase.version()
                    + " response has " + buffer.remaining() + " trailing byte(s)");
        }
        return new ParsedResponse(header, response);
    }

    static ObjectNode normalizeTypedResponse(
            TypedErrorCase testCase, AbstractResponse response, ResponseHeader header) {
        var normalized = JSON.createObjectNode();
        normalized.put("apiKey", testCase.apiKey().id);
        normalized.put("apiName", testCase.apiKey().name);
        normalized.put("requestVersion", testCase.version());
        normalized.put("correlationId", header.correlationId());
        normalized.put("responseHeaderVersion", header.headerVersion());
        normalized.set(
                "response",
                canonicalizeGeneratedJson(responseJson(response, testCase.version())));
        var taggedFields = normalized.putObject("taggedFields");
        taggedFields.set("responseHeader", rawTags(header.data().unknownTaggedFields()));
        taggedFields.set("response", rawTags(response.data().unknownTaggedFields()));
        taggedFields.set(
                "nestedResponse",
                canonicalizeGeneratedJson(nestedResponseTags(response, testCase.version())));
        return normalized;
    }

    private static ObjectNode nestedResponseTags(AbstractResponse response, short version) {
        return switch (response.apiKey()) {
            case METADATA -> version >= 9
                    ? metadataResponseTags((MetadataResponseData) response.data())
                    : JSON.createObjectNode();
            case OFFSET_COMMIT -> version >= 8
                    ? offsetCommitResponseTags((OffsetCommitResponseData) response.data())
                    : JSON.createObjectNode();
            case OFFSET_FETCH -> version >= 6
                    ? offsetFetchResponseTags((OffsetFetchResponseData) response.data(), version)
                    : JSON.createObjectNode();
            case JOIN_GROUP -> version >= 6
                    ? joinGroupResponseTags((JoinGroupResponseData) response.data())
                    : JSON.createObjectNode();
            case LEAVE_GROUP -> version >= 4
                    ? leaveGroupResponseTags((LeaveGroupResponseData) response.data())
                    : JSON.createObjectNode();
            case CREATE_TOPICS -> version >= 5
                    ? createTopicsResponseTags((CreateTopicsResponseData) response.data())
                    : JSON.createObjectNode();
            default -> JSON.createObjectNode();
        };
    }

    private static ObjectNode metadataResponseTags(MetadataResponseData data) {
        var result = JSON.createObjectNode();
        var brokers = result.putArray("brokers");
        for (var broker : data.brokers()) {
            var entry = tagNode("nodeId", broker.nodeId(), broker.unknownTaggedFields());
            brokers.add(entry);
        }
        var topics = result.putArray("topics");
        for (var topic : data.topics()) {
            var entry = tagNode("name", topic.name(), topic.unknownTaggedFields());
            var partitions = entry.putArray("partitions");
            for (var partition : topic.partitions()) {
                var partitionEntry = tagNode(
                        "partitionIndex",
                        partition.partitionIndex(),
                        partition.unknownTaggedFields());
                partitions.add(partitionEntry);
            }
            topics.add(entry);
        }
        return result;
    }

    private static ObjectNode offsetCommitResponseTags(OffsetCommitResponseData data) {
        var result = JSON.createObjectNode();
        var topics = result.putArray("topics");
        for (var topic : data.topics()) {
            var entry = tagNode("name", topic.name(), topic.unknownTaggedFields());
            var partitions = entry.putArray("partitions");
            for (var partition : topic.partitions()) {
                var partitionEntry = tagNode(
                        "partitionIndex",
                        partition.partitionIndex(),
                        partition.unknownTaggedFields());
                partitions.add(partitionEntry);
            }
            topics.add(entry);
        }
        return result;
    }

    private static ObjectNode offsetFetchResponseTags(
            OffsetFetchResponseData data, short version) {
        var result = JSON.createObjectNode();
        if (version < 8) {
            var topics = result.putArray("topics");
            for (var topic : data.topics()) {
                var entry = tagNode("name", topic.name(), topic.unknownTaggedFields());
                var partitions = entry.putArray("partitions");
                for (var partition : topic.partitions()) {
                    var partitionEntry = tagNode(
                            "partitionIndex",
                            partition.partitionIndex(),
                            partition.unknownTaggedFields());
                    partitions.add(partitionEntry);
                }
                topics.add(entry);
            }
        } else {
            var groups = result.putArray("groups");
            for (var group : data.groups()) {
                var entry = tagNode("groupId", group.groupId(), group.unknownTaggedFields());
                var topics = entry.putArray("topics");
                for (var topic : group.topics()) {
                    var topicEntry = tagNode("name", topic.name(), topic.unknownTaggedFields());
                    var partitions = topicEntry.putArray("partitions");
                    for (var partition : topic.partitions()) {
                        var partitionEntry = tagNode(
                                "partitionIndex",
                                partition.partitionIndex(),
                                partition.unknownTaggedFields());
                        partitions.add(partitionEntry);
                    }
                    topics.add(topicEntry);
                }
                groups.add(entry);
            }
        }
        return result;
    }

    private static ObjectNode joinGroupResponseTags(JoinGroupResponseData data) {
        var result = JSON.createObjectNode();
        var members = result.putArray("members");
        for (var member : data.members()) {
            var entry = tagNode("memberId", member.memberId(), member.unknownTaggedFields());
            members.add(entry);
        }
        return result;
    }

    private static ObjectNode leaveGroupResponseTags(LeaveGroupResponseData data) {
        var result = JSON.createObjectNode();
        var members = result.putArray("members");
        for (var member : data.members()) {
            var entry = tagNode("memberId", member.memberId(), member.unknownTaggedFields());
            members.add(entry);
        }
        return result;
    }

    private static ObjectNode createTopicsResponseTags(CreateTopicsResponseData data) {
        var result = JSON.createObjectNode();
        var topics = result.putArray("topics");
        for (var topic : data.topics()) {
            var entry = tagNode("name", topic.name(), topic.unknownTaggedFields());
            var topicConfigError = entry.putObject("knownTaggedFields")
                    .putObject("topicConfigErrorCode");
            topicConfigError.put("tag", 0);
            topicConfigError.put("value", topic.topicConfigErrorCode());
            var configs = entry.putArray("configs");
            for (var config : topic.configs()) {
                var configEntry = tagNode("name", config.name(), config.unknownTaggedFields());
                configs.add(configEntry);
            }
            topics.add(entry);
        }
        return result;
    }

    private static ObjectNode tagNode(
            String identityName, String identity, List<RawTaggedField> fields) {
        var node = JSON.createObjectNode();
        node.put(identityName, identity);
        node.set("unknownTaggedFields", rawTags(fields));
        return node;
    }

    private static ObjectNode tagNode(
            String identityName, int identity, List<RawTaggedField> fields) {
        var node = JSON.createObjectNode();
        node.put(identityName, identity);
        node.set("unknownTaggedFields", rawTags(fields));
        return node;
    }

    static JsonNode canonicalizeGeneratedJson(JsonNode node) {
        if (node.isObject()) {
            var canonical = JSON.createObjectNode();
            for (var entry : node.properties()) {
                if (entry.getKey().equals("errorMessage") && !entry.getValue().isNull()) {
                    canonical.put(entry.getKey(), "<present>");
                } else {
                    canonical.set(entry.getKey(), canonicalizeGeneratedJson(entry.getValue()));
                }
            }
            return canonical;
        }
        if (node.isArray()) {
            var children = new ArrayList<JsonNode>(node.size());
            node.forEach(child -> children.add(canonicalizeGeneratedJson(child)));
            children.sort(Comparator.comparing(JsonNode::toString));
            var canonical = JSON.createArrayNode();
            children.forEach(canonical::add);
            return canonical;
        }
        return node.deepCopy();
    }

    private static JsonNode responseJson(AbstractResponse response, short version) {
        return switch (response.apiKey()) {
            case PRODUCE -> ProduceResponseDataJsonConverter.write(
                    (ProduceResponseData) response.data(), version, false);
            case FETCH -> FetchResponseDataJsonConverter.write(
                    (FetchResponseData) response.data(), version, false);
            case LIST_OFFSETS -> ListOffsetsResponseDataJsonConverter.write(
                    (ListOffsetsResponseData) response.data(), version, false);
            case METADATA -> MetadataResponseDataJsonConverter.write(
                    (MetadataResponseData) response.data(), version, false);
            case OFFSET_COMMIT -> OffsetCommitResponseDataJsonConverter.write(
                    (OffsetCommitResponseData) response.data(), version, false);
            case OFFSET_FETCH -> OffsetFetchResponseDataJsonConverter.write(
                    (OffsetFetchResponseData) response.data(), version, false);
            case FIND_COORDINATOR -> FindCoordinatorResponseDataJsonConverter.write(
                    (FindCoordinatorResponseData) response.data(), version, false);
            case JOIN_GROUP -> JoinGroupResponseDataJsonConverter.write(
                    (JoinGroupResponseData) response.data(), version, false);
            case HEARTBEAT -> HeartbeatResponseDataJsonConverter.write(
                    (HeartbeatResponseData) response.data(), version, false);
            case LEAVE_GROUP -> LeaveGroupResponseDataJsonConverter.write(
                    (LeaveGroupResponseData) response.data(), version, false);
            case SYNC_GROUP -> SyncGroupResponseDataJsonConverter.write(
                    (SyncGroupResponseData) response.data(), version, false);
            case DESCRIBE_GROUPS -> DescribeGroupsResponseDataJsonConverter.write(
                    (DescribeGroupsResponseData) response.data(), version, false);
            case LIST_GROUPS -> ListGroupsResponseDataJsonConverter.write(
                    (ListGroupsResponseData) response.data(), version, false);
            case API_VERSIONS -> ApiVersionsResponseDataJsonConverter.write(
                    (ApiVersionsResponseData) response.data(), version, false);
            case CREATE_TOPICS -> CreateTopicsResponseDataJsonConverter.write(
                    (CreateTopicsResponseData) response.data(), version, false);
            case INIT_PRODUCER_ID -> InitProducerIdResponseDataJsonConverter.write(
                    (InitProducerIdResponseData) response.data(), version, false);
            case DESCRIBE_CONFIGS -> DescribeConfigsResponseDataJsonConverter.write(
                    (DescribeConfigsResponseData) response.data(), version, false);
            default -> throw new IllegalArgumentException("unexpected response API: " + response.apiKey());
        };
    }

    private static ArrayNode rawTags(List<RawTaggedField> fields) {
        var result = JSON.createArrayNode();
        var ordered = fields.stream()
                .sorted(Comparator.comparingInt(RawTaggedField::tag))
                .toList();
        for (var field : ordered) {
            var entry = result.addObject();
            entry.put("tag", field.tag());
            entry.put("size", field.size());
            entry.put("data", HexFormat.of().formatHex(field.data()));
        }
        return result;
    }

    static void requireNormalizedMatch(
            ApiKeys apiKey, short version, JsonNode expected, JsonNode actual) {
        if (expected.equals(actual)) {
            return;
        }
        var path = firstDifference(expected, actual, "$");
        throw new IllegalStateException(apiKey + " v" + version
                + " mismatch at " + path
                + ": expected=" + nodeAt(expected, path)
                + " actual=" + nodeAt(actual, path));
    }

    private static String firstDifference(JsonNode expected, JsonNode actual, String path) {
        if (expected == null || actual == null || expected.getNodeType() != actual.getNodeType()) {
            return path;
        }
        if (expected.isObject()) {
            var expectedNames = fieldNames(expected);
            var actualNames = fieldNames(actual);
            if (!expectedNames.equals(actualNames)) {
                for (var name : expectedNames) {
                    if (!actual.has(name)) return path + "." + name;
                }
                for (var name : actualNames) {
                    if (!expected.has(name)) return path + "." + name;
                }
                return path;
            }
            for (var name : expectedNames) {
                var child = firstDifference(expected.get(name), actual.get(name), path + "." + name);
                if (child != null) return child;
            }
            return null;
        }
        if (expected.isArray()) {
            if (expected.size() != actual.size()) return path + ".length";
            for (var index = 0; index < expected.size(); index++) {
                var child = firstDifference(expected.get(index), actual.get(index), path + "[" + index + "]");
                if (child != null) return child;
            }
            return null;
        }
        return expected.equals(actual) ? null : path;
    }

    private static List<String> fieldNames(JsonNode node) {
        var names = new ArrayList<String>();
        Iterator<String> iterator = node.fieldNames();
        iterator.forEachRemaining(names::add);
        return names;
    }

    private static JsonNode nodeAt(JsonNode root, String path) {
        if (path == null || path.equals("$")) return root;
        var pointer = path.substring(1)
                .replaceAll("\\[([0-9]+)]", "/$1")
                .replace('.', '/');
        if (pointer.endsWith("/length")) {
            var arrayPointer = pointer.substring(0, pointer.length() - "/length".length());
            return JSON.getNodeFactory().numberNode(root.at(arrayPointer).size());
        }
        return root.at(pointer);
    }

    private static void runApiVersions(CommandLine command) throws Exception {
        var header = new RequestHeader(
                ApiKeys.API_VERSIONS,
                command.version(),
                CLIENT_ID,
                API_VERSIONS_CORRELATION_ID);
        var bytes = MessageUtil.toByteBufferAccessor(header.data(), header.headerVersion()).buffer();
        var response = exchange(command.bootstrapServer(), bytes);
        var detected = detectApiVersionsEncoding(response);
        var output = JSON.createObjectNode();
        output.put("requestedVersion", command.version());
        output.put("responseHeaderVersion", detected.responseHeaderVersion());
        output.put("decodedBodyVersion", detected.decodedBodyVersion());
        output.put("correlationId", detected.correlationId());
        output.put("error", detected.error().name());
        writeJson(command.output(), output);
        System.out.println("ApiVersions v" + command.version()
                + " compatible decoded body versions=" + detected.compatibleBodyVersions()
                + "; canonical=" + detected.decodedBodyVersion());
    }

    static DetectedEncoding detectApiVersionsEncoding(byte[] bytes) {
        var candidates = new ArrayList<EncodingCandidate>();
        for (short bodyVersion = ApiKeys.API_VERSIONS.oldestVersion();
                bodyVersion <= ApiKeys.API_VERSIONS.latestVersion(false);
                bodyVersion++) {
            var headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(bodyVersion);
            try {
                var buffer = ByteBuffer.wrap(bytes);
                var header = ResponseHeader.parse(buffer, headerVersion);
                var response = (ApiVersionsResponseData) AbstractResponse.parseResponse(
                                ApiKeys.API_VERSIONS, new ByteBufferAccessor(buffer), bodyVersion)
                        .data();
                if (!buffer.hasRemaining()
                        && header.correlationId() == API_VERSIONS_CORRELATION_ID
                        && Errors.forCode(response.errorCode()) == Errors.UNSUPPORTED_VERSION) {
                    var exactBodyVersions = exactApiVersionsEncodings(bytes, header, response);
                    if (!exactBodyVersions.isEmpty()) {
                        var semantics = canonicalizeGeneratedJson(
                                ApiVersionsResponseDataJsonConverter.write(
                                        response, ApiKeys.API_VERSIONS.latestVersion(false), false));
                        candidates.add(new EncodingCandidate(
                                headerVersion,
                                bodyVersion,
                                header.correlationId(),
                                Errors.forCode(response.errorCode()),
                                semantics,
                                exactBodyVersions));
                    }
                }
            } catch (RuntimeException ignored) {
                // A candidate is valid only when Kafka's generated decoder fully accepts it.
            }
        }
        return selectUniqueEncoding(candidates);
    }

    private static List<Short> exactApiVersionsEncodings(
            byte[] observed, ResponseHeader header, ApiVersionsResponseData response) {
        var exact = new ArrayList<Short>();
        for (short encodingVersion = ApiKeys.API_VERSIONS.oldestVersion();
                encodingVersion <= ApiKeys.API_VERSIONS.latestVersion(false);
                encodingVersion++) {
            if (Arrays.equals(
                    observed,
                    serializeApiVersionsResponse(header.correlationId(), response, encodingVersion))) {
                exact.add(encodingVersion);
            }
        }
        return List.copyOf(exact);
    }

    private static byte[] serializeApiVersionsResponse(
            int correlationId, ApiVersionsResponseData response, short bodyVersion) {
        var headerVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(bodyVersion);
        var header = MessageUtil.toByteBufferAccessor(
                        new ResponseHeader(correlationId, headerVersion).data(), headerVersion)
                .buffer();
        var body = MessageUtil.toByteBufferAccessor(response, bodyVersion).buffer();
        var serialized = ByteBuffer.allocate(header.remaining() + body.remaining());
        serialized.put(header).put(body);
        return serialized.array();
    }

    static DetectedEncoding selectUniqueEncoding(List<EncodingCandidate> candidates) {
        if (candidates.isEmpty()) {
            throw new IllegalStateException("no valid Kafka 4.3.1 ApiVersions response encoding");
        }
        var groups = new LinkedHashMap<EncodingEquivalence, List<EncodingCandidate>>();
        for (var candidate : candidates) {
            var equivalence = new EncodingEquivalence(
                    candidate.responseHeaderVersion(),
                    candidate.correlationId(),
                    candidate.error(),
                    candidate.semanticContent(),
                    candidate.exactBodyVersions());
            groups.computeIfAbsent(equivalence, ignored -> new ArrayList<>()).add(candidate);
        }
        if (groups.size() != 1) {
            throw new IllegalStateException("ambiguous Kafka 4.3.1 ApiVersions response encodings: "
                    + candidates.stream()
                            .map(candidate -> candidate.responseHeaderVersion()
                                    + "/" + candidate.parserBodyVersion())
                            .toList());
        }
        var equivalent = groups.values().iterator().next();
        var compatibleBodyVersions = equivalent.stream()
                .map(EncodingCandidate::parserBodyVersion)
                .distinct()
                .sorted()
                .toList();
        var canonical = equivalent.stream()
                .min(Comparator.comparingInt(EncodingCandidate::parserBodyVersion))
                .orElseThrow();
        return new DetectedEncoding(
                canonical.responseHeaderVersion(),
                compatibleBodyVersions.getFirst(),
                canonical.correlationId(),
                canonical.error(),
                compatibleBodyVersions);
    }

    private static byte[] exchange(HostPort server, ByteBuffer payload) throws IOException {
        try (var socket = new Socket()) {
            socket.connect(new InetSocketAddress(server.host(), server.port()), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            var output = new DataOutputStream(socket.getOutputStream());
            var request = payload.duplicate();
            output.writeInt(request.remaining());
            if (request.hasArray()) {
                output.write(
                        request.array(),
                        request.arrayOffset() + request.position(),
                        request.remaining());
            } else {
                var bytes = new byte[request.remaining()];
                request.get(bytes);
                output.write(bytes);
            }
            output.flush();
            return readFrame(new DataInputStream(socket.getInputStream()));
        }
    }

    static byte[] readFrame(DataInputStream input) throws IOException {
        var length = input.readInt();
        if (length < 0) {
            throw new IOException("negative response frame length: " + length);
        }
        if (length > MAX_RESPONSE_BYTES) {
            throw new IOException("oversized response frame length: " + length);
        }
        var frame = new byte[length];
        try {
            input.readFully(frame);
        } catch (EOFException failure) {
            throw new EOFException("truncated response frame: expected " + length + " byte(s)");
        }
        return frame;
    }

    private static void writeJson(Path output, JsonNode value) throws IOException {
        var content = JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value) + "\n";
        Files.writeString(
                output,
                content,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
    }

    enum Subcommand { TYPED_ERRORS, API_VERSIONS }

    record HostPort(String host, int port) {}

    record CommandLine(Subcommand subcommand, HostPort bootstrapServer, Short version, Path output) {}

    record TypedErrorCase(ApiKeys apiKey, short version, int correlationId, AbstractRequest request) {}

    record ParsedResponse(ResponseHeader header, AbstractResponse response) {}

    record DetectedEncoding(
            short responseHeaderVersion,
            short decodedBodyVersion,
            int correlationId,
            Errors error,
            List<Short> compatibleBodyVersions) {}

    record EncodingCandidate(
            short responseHeaderVersion,
            short parserBodyVersion,
            int correlationId,
            Errors error,
            JsonNode semanticContent,
            List<Short> exactBodyVersions) {}

    private record EncodingEquivalence(
            short responseHeaderVersion,
            int correlationId,
            Errors error,
            JsonNode semanticContent,
            List<Short> exactBodyVersions) {}
}
