# Frequently Asked Questions (FAQ)

---

## Stream Modality

Questions related to stream modality (e.g., [MultiStreamTracker][multi-stream-tracker]).

### What is the impact of transitioning my KCL app from single-stream to multi-stream?

This answer assumes the [StreamTracker][stream-tracker] implementation is being changed.
From KCL's perspective, there is no modality change by decreasing a multi-stream tracker from `N` streams to `1` stream.

The DDB `leaseKey`, used to persist metadata including lease checkpoint, has a modality-dependent format:

| Modality | `leaseKey` Format |
| --- | --- |
| single-stream | `<shardId>` |
| multi-stream  | `<accountId>:<streamName>:<streamCreationTimestamp>:<shardId>` |

Transitioning an app -- either from single- to multi-, or vice versa -- creates a backwards-incompatible expectation on the `leaseKey`.
As a result, a KCL app will be blind to any `leaseKey`, and its checkpoint, that does not match the expected format.
For leases that don't exist in the expected format, processing may start from the default checkpoint (e.g., `LATEST`).

As an example of potential impact from switching modality, assume `LATEST` is the default initial position in stream.
When a KCL application's modality is switched, stream processing will start reading at this initial position ignoring the checkpoints from the previous modality.
The impact of this is that any records written to the stream between restarting the KCL app will not be processed.
If `TRIM_HORIZON` is used instead upon restarting the application, the application will start reading from its initial position solving for the gap in consumption of data but potentially resulting in duplicate consumption of records.
Thus, please make sure that your application can handle this.

---

## Resources

For additional information, please consider reading:
* https://docs.aws.amazon.com/streams/latest/dev/kcl-migration.html
* https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html

[multi-stream-tracker]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/processor/MultiStreamTracker.java
[stream-tracker]: /amazon-kinesis-client/src/main/java/software/amazon/kinesis/processor/StreamTracker.java