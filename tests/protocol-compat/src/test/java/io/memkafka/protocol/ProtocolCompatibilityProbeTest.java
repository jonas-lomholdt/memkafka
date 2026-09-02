package io.memkafka.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ProtocolCompatibilityProbeTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path temporaryDirectory;

    @Test
    void missingTypedErrorCaseCannotPassCoverageValidation() {
        var cases = new ArrayList<>(ProtocolCompatibilityProbe.typedErrorCases());
        assertEquals(27, cases.size(), "fixture must begin with the complete adjacent matrix");
        assertTrue(cases.stream().anyMatch(testCase ->
                testCase.apiKey() == ApiKeys.DESCRIBE_CLUSTER && testCase.version() == 1));
        assertTrue(cases.stream().noneMatch(testCase ->
                testCase.apiKey() == ApiKeys.METADATA && testCase.version() == 10));
        assertTrue(cases.stream().noneMatch(testCase ->
                testCase.apiKey() == ApiKeys.CREATE_TOPICS && testCase.version() == 7));
        cases.removeLast();

        var failure = assertThrows(
                IllegalStateException.class,
                () -> ProtocolCompatibilityProbe.validateTypedErrorCases(cases));

        assertTrue(failure.getMessage().contains("27 unique cases"));
    }

    @Test
    void supportedSemanticCasesCoverMetadataV10ThroughV13AndEndpointErrors() {
        var cases = ProtocolCompatibilityProbe.supportedSemanticCases();

        assertEquals(
                List.of(
                        "metadata-request-error-v10",
                        "metadata-request-error-v11",
                        "metadata-request-error-v12",
                        "metadata-request-error-v13",
                        "metadata-uuid-set-semantics",
                        "metadata-name-set-semantics",
                        "describe-cluster-controller-endpoint",
                        "describe-cluster-unknown-endpoint"),
                cases.stream().map(ProtocolCompatibilityProbe.SupportedSemanticCase::name).toList());
        assertEquals(
                List.of((short) 10, (short) 11, (short) 12, (short) 13, (short) 13, (short) 13),
                cases.stream()
                        .filter(testCase -> testCase.apiKey() == ApiKeys.METADATA)
                        .map(ProtocolCompatibilityProbe.SupportedSemanticCase::version)
                        .toList());
        assertEquals(
                List.of((byte) 2, (byte) 99),
                cases.stream()
                        .filter(testCase -> testCase.apiKey() == ApiKeys.DESCRIBE_CLUSTER)
                        .map(testCase -> testCase.request().data())
                        .map(data -> ((org.apache.kafka.common.message.DescribeClusterRequestData) data)
                                .endpointType())
                        .toList());
    }

    @Test
    void everySupportedSemanticRequestSerializesAtItsDeclaredVersion() {
        for (var testCase : ProtocolCompatibilityProbe.supportedSemanticCases()) {
            assertDoesNotThrow(
                    () -> testCase.request().serializeWithHeader(new RequestHeader(
                            testCase.apiKey(),
                            testCase.version(),
                            "test-client",
                            testCase.correlationId())),
                    testCase.name());
        }
    }

    @Test
    void metadataRequestErrorOracleUsesKafka431WireDefaultsAcrossSupportedVersions() {
        for (short version = 10; version <= 13; version++) {
            var request = ProtocolCompatibilityProbe.metadataRequestErrorOracle(version);
            var expectedError = version <= 11
                    ? Errors.INVALID_REQUEST
                    : Errors.UNKNOWN_SERVER_ERROR;
            var exception = version <= 11
                    ? new InvalidRequestException("semantic oracle")
                    : new NullPointerException("semantic oracle");
            var response = request.getErrorResponse(0, exception);
            var encoded = MessageUtil.toByteBufferAccessor(
                            response.data(), version)
                    .buffer();
            var decoded = (MetadataResponseData) AbstractResponse.parseResponse(
                            ApiKeys.METADATA,
                            new ByteBufferAccessor(encoded),
                            version)
                    .data();

            assertTrue(decoded.brokers().isEmpty(), "v" + version);
            assertEquals(null, decoded.clusterId(), "v" + version);
            assertEquals(-1, decoded.controllerId(), "v" + version);
            assertEquals(Integer.MIN_VALUE, decoded.clusterAuthorizedOperations(), "v" + version);
            var topics = new ArrayList<MetadataResponseData.MetadataResponseTopic>();
            decoded.topics().forEach(topics::add);
            assertEquals(2, topics.size(), "v" + version);
            assertEquals("oracle-metadata-request-error", topics.get(0).name(), "v" + version);
            assertEquals(
                    new Uuid(0x1234567812345678L, 0x9abcdef012345678L),
                    topics.get(0).topicId(),
                    "v" + version);
            assertEquals("", topics.get(1).name(), "v" + version);
            assertEquals(Uuid.ZERO_UUID, topics.get(1).topicId(), "v" + version);
            for (var topic : topics) {
                assertEquals(expectedError.code(), topic.errorCode(), "v" + version);
                assertEquals(Integer.MIN_VALUE, topic.topicAuthorizedOperations(), "v" + version);
                assertTrue(topic.partitions().isEmpty(), "v" + version);
            }
            assertEquals(
                    version == 13 ? expectedError.code() : Errors.NONE.code(),
                    decoded.errorCode(),
                    "v" + version);
        }
    }

    @Test
    void supportedSemanticNormalizationPreservesOrderAndExactMessages() {
        var testCase = ProtocolCompatibilityProbe.supportedSemanticCases().stream()
                .filter(candidate -> candidate.name().equals("describe-cluster-controller-endpoint"))
                .findFirst()
                .orElseThrow();
        var response = new DescribeClusterResponseData()
                .setErrorCode(Errors.MISMATCHED_ENDPOINT_TYPE.code())
                .setErrorMessage("exact Kafka endpoint message");

        var normalized = ProtocolCompatibilityProbe.normalizeSupportedResponse(
                testCase,
                new org.apache.kafka.common.requests.DescribeClusterResponse(response),
                new ResponseHeader(
                        testCase.correlationId(),
                        testCase.apiKey().responseHeaderVersion(testCase.version())));

        assertEquals(
                "exact Kafka endpoint message",
                normalized.at("/response/errorMessage").textValue());
    }

    @Test
    void normalizedMismatchNamesTheFirstFieldPath() throws Exception {
        var expected = JSON.readTree("""
                {"response":{"topics":[{"name":"alpha","errorCode":35},{"name":"beta","errorCode":35}]}}
                """);
        var actual = JSON.readTree("""
                {"response":{"topics":[{"name":"alpha","errorCode":35},{"name":"beta","errorCode":0}]}}
                """);

        var failure = assertThrows(
                IllegalStateException.class,
                () -> ProtocolCompatibilityProbe.requireNormalizedMatch(
                        ApiKeys.METADATA, (short) 10, expected, actual));

        assertTrue(failure.getMessage().contains("METADATA v10"));
        assertTrue(failure.getMessage().contains("$.response.topics[1].errorCode"));
    }

    @Test
    void arrayLengthMismatchReportsExpectedAndActualCounts() throws Exception {
        var expected = JSON.readTree("""
                {"response":{"topics":[]}}
                """);
        var actual = JSON.readTree("""
                {"response":{"topics":[{}, {}]}}
                """);

        var failure = assertThrows(
                IllegalStateException.class,
                () -> ProtocolCompatibilityProbe.requireNormalizedMatch(
                        ApiKeys.OFFSET_FETCH, (short) 4, expected, actual));

        assertTrue(failure.getMessage().contains("$.response.topics.length"));
        assertTrue(failure.getMessage().contains("expected=0 actual=2"));
    }

    @Test
    void cliRejectsMissingDuplicateUnknownAndInvalidArguments() throws IOException {
        var output = temporaryDirectory.resolve("result.json");
        var invalid = List.of(
                new String[] {"typed-errors", "--bootstrap-server", "localhost:9092"},
                new String[] {"typed-errors", "--bootstrap-server", "localhost:9092",
                        "--bootstrap-server", "other:9092", "--output", output.toString()},
                new String[] {"typed-errors", "--bootstrap-server", "localhost:9092",
                        "--mystery", "value", "--output", output.toString()},
                new String[] {"typed-errors", "--bootstrap-server", "missing-port",
                        "--output", output.toString()},
                new String[] {"api-versions", "--bootstrap-server", "localhost:9092",
                        "--version", "32768", "--output", output.toString()},
                new String[] {"api-versions", "--bootstrap-server", "localhost:9092",
                        "--version", "5", "--output", temporaryDirectory.toString()});

        for (var arguments : invalid) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> ProtocolCompatibilityProbe.parseCommandLine(arguments),
                    String.join(" ", arguments));
        }
    }

    @Test
    void frameReaderRejectsTruncatedAndOversizedResponses() throws Exception {
        var truncatedBytes = ByteBuffer.allocate(7).putInt(8).put(new byte[] {1, 2, 3}).array();
        assertThrows(
                EOFException.class,
                () -> ProtocolCompatibilityProbe.readFrame(
                        new DataInputStream(new ByteArrayInputStream(truncatedBytes))));

        var oversizedBytes = ByteBuffer.allocate(4)
                .putInt(ProtocolCompatibilityProbe.MAX_RESPONSE_BYTES + 1)
                .array();
        var oversized = assertThrows(
                IOException.class,
                () -> ProtocolCompatibilityProbe.readFrame(
                        new DataInputStream(new ByteArrayInputStream(oversizedBytes))));
        assertTrue(oversized.getMessage().contains("oversized"));
    }

    @Test
    void apiVersionsDetectionRejectsNoCandidateAndNonEquivalentCandidates() throws Exception {
        var absent = assertThrows(
                IllegalStateException.class,
                () -> ProtocolCompatibilityProbe.selectUniqueEncoding(List.of()));
        assertTrue(absent.getMessage().contains("no valid"));

        var first = new ProtocolCompatibilityProbe.EncodingCandidate(
                (short) 0, (short) 0, 1234, Errors.UNSUPPORTED_VERSION,
                JSON.readTree("{\"apiKeys\":[]}"), List.of((short) 0));
        var differentSemantics = new ProtocolCompatibilityProbe.EncodingCandidate(
                (short) 0, (short) 1, 1234, Errors.UNSUPPORTED_VERSION,
                JSON.readTree("{\"apiKeys\":[{}]}"), List.of((short) 0));
        var semanticAmbiguity = assertThrows(
                IllegalStateException.class,
                () -> ProtocolCompatibilityProbe.selectUniqueEncoding(List.of(first, differentSemantics)));
        assertTrue(semanticAmbiguity.getMessage().contains("ambiguous"));

        var differentReencoding = new ProtocolCompatibilityProbe.EncodingCandidate(
                (short) 0, (short) 1, 1234, Errors.UNSUPPORTED_VERSION,
                JSON.readTree("{\"apiKeys\":[]}"), List.of((short) 1));
        var wireAmbiguity = assertThrows(
                IllegalStateException.class,
                () -> ProtocolCompatibilityProbe.selectUniqueEncoding(List.of(first, differentReencoding)));
        assertTrue(wireAmbiguity.getMessage().contains("ambiguous"));
    }

    @Test
    void apiVersionsDetectionCollapsesEquivalentKafkaParserFallbacksToCanonicalZero() {
        var data = new ApiVersionsResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code());
        data.apiKeys().add(new ApiVersionsResponseData.ApiVersion()
                .setApiKey(ApiKeys.API_VERSIONS.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 4));
        var header = MessageUtil.toByteBufferAccessor(
                        new ResponseHeaderData().setCorrelationId(1234), (short) 0)
                .buffer();
        var body = MessageUtil.toByteBufferAccessor(data, (short) 0).buffer();
        var frame = ByteBuffer.allocate(header.remaining() + body.remaining())
                .put(header)
                .put(body)
                .array();

        var detected = ProtocolCompatibilityProbe.detectApiVersionsEncoding(frame);

        assertEquals(0, detected.responseHeaderVersion());
        assertEquals(0, detected.decodedBodyVersion());
        assertEquals(Errors.UNSUPPORTED_VERSION, detected.error());
        assertEquals(List.of((short) 0, (short) 1, (short) 2), detected.compatibleBodyVersions());
    }

    @Test
    void generatedResponseArraysAreCanonicalizedRecursively() throws Exception {
        var response = JSON.readTree("""
                {"topics":[
                  {"name":"beta","partitions":[{"index":3},{"index":1}]},
                  {"name":"alpha","partitions":[{"index":4},{"index":2}]}
                ]}
                """);

        var canonical = ProtocolCompatibilityProbe.canonicalizeGeneratedJson(response);

        assertEquals("alpha", canonical.at("/topics/0/name").textValue());
        assertEquals(2, canonical.at("/topics/0/partitions/0/index").intValue());
        assertEquals("beta", canonical.at("/topics/1/name").textValue());
        assertEquals(1, canonical.at("/topics/1/partitions/0/index").intValue());
    }

    @Test
    void errorMessagesComparePresenceWithoutLeakingOracleText() throws Exception {
        var response = JSON.readTree("""
                {"present":{"errorMessage":"MemKafka compatibility oracle"},
                 "absent":{"errorMessage":null}}
                """);

        var canonical = ProtocolCompatibilityProbe.canonicalizeGeneratedJson(response);

        assertEquals("<present>", canonical.at("/present/errorMessage").textValue());
        assertTrue(canonical.at("/absent/errorMessage").isNull());
        assertTrue(!canonical.toString().contains("MemKafka compatibility oracle"));
    }

    @Test
    void unexpectedNestedTaggedFieldProducesANormalizedMismatch() {
        var testCase = typedCase(ApiKeys.METADATA, (short) 10);
        var response = testCase.request().getErrorResponse(
                0, new UnsupportedVersionException("test"));
        var header = responseHeader(testCase);
        var expected = ProtocolCompatibilityProbe.normalizeTypedResponse(
                testCase, response, header);

        var data = (MetadataResponseData) response.data();
        data.topics().iterator().next().unknownTaggedFields()
                .add(new RawTaggedField(777, new byte[] {1, 2, 3}));
        var actual = ProtocolCompatibilityProbe.normalizeTypedResponse(
                testCase, response, header);

        var failure = assertThrows(
                IllegalStateException.class,
                () -> ProtocolCompatibilityProbe.requireNormalizedMatch(
                        testCase.apiKey(), testCase.version(), expected, actual));
        assertTrue(
                failure.getMessage().contains(
                        "$.taggedFields.nestedResponse.topics[0].unknownTaggedFields.length"),
                failure.getMessage());
    }

    @Test
    void defaultValuedKnownTaggedFieldIsPresentInNormalizedEvidence() {
        var testCase = typedCase(ApiKeys.CREATE_TOPICS, (short) 7);
        var response = testCase.request().getErrorResponse(
                0, new UnsupportedVersionException("test"));

        var normalized = ProtocolCompatibilityProbe.normalizeTypedResponse(
                testCase, response, responseHeader(testCase));

        var firstTopicConfigError = normalized.at(
                "/taggedFields/nestedResponse/topics/0/knownTaggedFields/"
                        + "topicConfigErrorCode");
        assertTrue(!firstTopicConfigError.isMissingNode());
        assertEquals(0, firstTopicConfigError.at("/tag").intValue());
        assertEquals(0, firstTopicConfigError.at("/value").intValue());
    }

    @Test
    void everyTypedCaseSerializesAndProducesAVersionEncodableError() {
        for (var testCase : ProtocolCompatibilityProbe.typedErrorCases()) {
            assertDoesNotThrow(() -> {
                var header = new RequestHeader(
                        testCase.apiKey(),
                        testCase.version(),
                        "test-client",
                        testCase.correlationId());
                testCase.request().serializeWithHeader(header);
                var response = testCase.request().getErrorResponse(
                        0, new UnsupportedVersionException("test"));
                assertNotNull(response);
                MessageUtil.toByteBufferAccessor(response.data(), testCase.version());
            }, testCase.apiKey() + " v" + testCase.version());
        }
    }

    private static ProtocolCompatibilityProbe.TypedErrorCase typedCase(
            ApiKeys apiKey, short version) {
        return ProtocolCompatibilityProbe.typedErrorCases().stream()
                .filter(testCase -> testCase.apiKey() == apiKey && testCase.version() == version)
                .findFirst()
                .orElseGet(() -> new ProtocolCompatibilityProbe.TypedErrorCase(
                        apiKey,
                        version,
                        12_345,
                        ProtocolCompatibilityProbe.buildTypedRequest(apiKey, version)));
    }

    private static ResponseHeader responseHeader(
            ProtocolCompatibilityProbe.TypedErrorCase testCase) {
        return new ResponseHeader(
                testCase.correlationId(),
                testCase.apiKey().responseHeaderVersion(testCase.version()));
    }
}
