//! Basic Periodic Metrics Processor
//!
//! This module provides a basic metrics processor that collects metrics from registered
//! meters on a periodic basis using a background thread.
//!
//! The processor maintains a thread that wakes up at regular intervals to collect
//! metrics from all registered meters and export them via the configured exporter.
//!
//! - A cycle is a collection (the observable callbacks, then the aggregation state) and its
//!   export. Cycles never overlap. The export runs outside the reader's mutex, so
//!   registering a meter or asking for shutdown never waits on the network.
//! - An export that succeeds is committed to the aggregation state; one that fails is not,
//!   and the next cycle's observable deltas cover it.
//! - `shutdown` wakes the thread at once, lets a cycle in flight finish, runs one last
//!   cycle inside its timeout and joins. How long that takes is the exporter's to bound.

const std = @import("std");
const io = std.Options.debug_io;
const api = @import("otel-api");

const sdk = struct {
    const BridgeReader = @import("reader.zig").BridgeReader;
    const Meter = @import("meter.zig").Meter;
    const MeterProvider = @import("meter_provider.zig").MeterProvider;
    const MetricData = @import("data.zig").MetricData;
    const Reader = @import("reader.zig").Reader;
    const MetricExporter = @import("exporter.zig").MetricExporter;
    const ReaderAggregationState = @import("reader_aggregation_state.zig").ReaderAggregationState;
    const MetricMetadata = @import("metadata.zig").MetricMetadata;
    const MetricValue = @import("reader.zig").MetricValue;
    const Resource = @import("../resource/resource.zig").Resource;
};

/// Basic periodic metrics processor that collects metrics at regular intervals
pub const PeriodicReader = struct {
    pub const PipelineStep = @import("../common/pipeline.zig").PipelineStepInstructions(
        PeriodicReader,
        sdk.Reader,
        ?u32,
        reader,
        _initFn,
        setExporter,
    );
    pub fn _initFn(self: *PeriodicReader, interval: ?u32, allocator: std.mem.Allocator) !void {
        self.* = try init(allocator, null, interval);
        try self.start();
    }

    allocator: std.mem.Allocator,
    exporter: ?sdk.MetricExporter,
    /// The registered meters and the collection itself; never held across an export.
    mutex: std.Io.Mutex,
    /// One cycle at a time: the thread's, `collect`'s and `forceFlush`'s.
    cycle_mutex: std.Io.Mutex,
    /// Set by `shutdown`: wakes the thread out of its interval.
    stop: std.Io.Event,
    is_shutdown: std.atomic.Value(bool),
    is_running: std.atomic.Value(bool),
    /// The bound on the thread's last cycle (`shutdown`'s timeout); 0: none.
    final_timeout_ms: std.atomic.Value(u64),
    last_collection_time: std.atomic.Value(i64),
    /// How long the last collection took (callbacks and aggregation, not the export).
    last_collection_duration_ns: std.atomic.Value(u64),
    thread: ?std.Thread,
    collection_interval_ms: u32,
    registered_meters: std.ArrayListUnmanaged(*sdk.Meter),
    reader_state: sdk.ReaderAggregationState,

    /// Initialize a new basic periodic metrics processor
    /// collection_interval_ms: How often to collect metrics (default: 60000ms = 60s)
    pub fn init(
        allocator: std.mem.Allocator,
        exporter: ?sdk.MetricExporter,
        collection_interval_ms: ?u32,
    ) !PeriodicReader {
        return .{
            .allocator = allocator,
            .exporter = exporter,
            .mutex = std.Io.Mutex.init,
            .cycle_mutex = std.Io.Mutex.init,
            .stop = .unset,
            .is_shutdown = std.atomic.Value(bool).init(false),
            .is_running = std.atomic.Value(bool).init(false),
            .final_timeout_ms = .init(0),
            .last_collection_time = std.atomic.Value(i64).init(0),
            .last_collection_duration_ns = .init(0),
            .thread = null,
            .collection_interval_ms = collection_interval_ms orelse 60000, // 60 seconds default
            .registered_meters = .empty,
            .reader_state = try sdk.ReaderAggregationState.init(
                allocator,
                .delta, // Default to Delta temporality for now
                @import("reader_aggregation_state.zig").defaultAggregationSelector,
            ),
        };
    }

    /// Start the background collection thread
    pub fn start(self: *PeriodicReader) !void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        if (self.is_running.load(.acquire) or self.thread != null or self.is_shutdown.load(.acquire)) {
            return;
        }

        self.is_running.store(true, .release);
        errdefer self.is_running.store(false, .release);
        self.thread = try std.Thread.spawn(.{}, collectionThreadFn, .{self});
    }

    /// Stop the background collection thread and clean up resources
    pub fn deinit(self: *PeriodicReader) void {
        _ = self.shutdown(null);

        // Clean up resources
        self.reader_state.deinit();
        self.registered_meters.deinit(self.allocator);
        if (self.exporter) |exporter| {
            exporter.deinit();
            exporter.destroy();
        }
    }

    /// Destroy the processor and free its memory
    pub fn destroy(self: *PeriodicReader) void {
        self.allocator.destroy(self);
    }

    pub fn recordMeasurement(
        self: *PeriodicReader,
        value: sdk.MetricValue,
        attributes: []const api.AttributeKeyValue,
        metadata: sdk.MetricMetadata,
        metadata_hash: u64,
    ) void {
        self.reader_state.recordMeasurement(value, attributes, metadata, metadata_hash);
    }

    fn nowNs() i96 {
        return std.Io.Timestamp.now(io, .awake).nanoseconds;
    }

    /// One cycle: collect, export within `timeout_ms` (null: the exporter's own limit),
    /// commit if it arrived.
    fn cycle(self: *PeriodicReader, timeout_ms: ?u64) api.common.ExportResult {
        self.cycle_mutex.lockUncancelable(io);
        defer self.cycle_mutex.unlock(io);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const collected_metrics = blk: {
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            const t0 = nowNs();
            defer self.last_collection_duration_ns.store(@intCast(@max(nowNs() - t0, 0)), .monotonic);

            // trigger the observables to write their data to the aggregation state.
            for (self.registered_meters.items) |meter| {
                meter.triggerObservables(allocator, self.reader());
            }
            // every meter of a reader is its provider's
            const resource = if (self.registered_meters.items.len > 0) self.registered_meters.items[0].provider.resource else sdk.Resource.empty;
            break :blk self.reader_state.collect(allocator, resource) catch |err| {
                api.common.reportError(.{
                    .component = .meter,
                    .context = null,
                    .error_type = .internal,
                    .message = "Failed to collect metrics",
                    .operation = "PeriodicReader.cycle",
                    .source_error = err,
                });
                return .failure;
            };
        };

        // Exporter must copy memory that it needs beyond the duration of this call.
        const result: api.common.ExportResult = if (self.exporter) |*exporter| exporter.exportMetricsWithin(collected_metrics, timeout_ms) else .success;
        if (result == .success) self.reader_state.commit();
        return result;
    }

    pub fn collect(self: *PeriodicReader) void {
        _ = self.cycle(null);
    }

    /// Collect and export now, then flush the exporter.
    pub fn forceFlush(self: *PeriodicReader, timeout_ms: ?u64) api.common.FlushResult {
        const t0 = nowNs();
        const exported = self.cycle(timeout_ms);
        if (exported != .success) return exported.asFlushResult();
        const exporter = self.exporter orelse return .success;
        if (timeout_ms) |limit| {
            const spent: u64 = @intCast(@divFloor(@max(nowNs() - t0, 0), std.time.ns_per_ms));
            if (spent >= limit) return .timeout;
            return exporter.forceFlush(limit - spent).asFlushResult();
        }
        return exporter.forceFlush(null).asFlushResult();
    }

    /// Stop collecting: a last cycle, bounded by `timeout_ms`, then the exporter's shutdown.
    /// Returns once the thread has ended.
    pub fn shutdown(self: *PeriodicReader, timeout_ms: ?u64) api.common.ProcessResult {
        if (self.is_shutdown.swap(true, .seq_cst)) {
            return .success;
        }
        self.final_timeout_ms.store(timeout_ms orelse 0, .release);
        self.stop.set(io);

        const thread = blk: {
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            const t = self.thread;
            self.thread = null;
            break :blk t;
        };
        // The thread runs the last cycle; a reader that never started one runs it here.
        if (thread) |t| t.join() else _ = self.cycle(timeout_ms);
        self.is_running.store(false, .release);

        // Shutdown the exporter
        const result = if (self.exporter) |*exporter| exporter.shutdown(timeout_ms) else .success;
        return if (result == .success) .success else .failure;
    }

    /// Register a meter for periodic collection
    pub fn registerMeter(self: *PeriodicReader, meter: *sdk.Meter) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        if (self.is_shutdown.load(.acquire)) return;

        self.registered_meters.append(self.allocator, meter) catch {
            // Handle allocation failure silently for now
            return;
        };
    }

    /// Unregister a meter from periodic collection
    pub fn unregisterMeter(self: *PeriodicReader, meter: *sdk.Meter) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        if (self.is_shutdown.load(.acquire)) return;

        for (self.registered_meters.items, 0..) |registered_meter, i| {
            if (registered_meter == meter) {
                _ = self.registered_meters.swapRemove(i);
                break;
            }
        }
    }

    /// Unregister all meters from periodic collection
    pub fn unregisterAllMeters(self: *PeriodicReader) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        self.registered_meters.clearAndFree(self.allocator);
    }

    /// Set the exporter for this processor
    pub fn setExporter(self: *PeriodicReader, exporter: ?sdk.MetricExporter) !void {
        if (self.exporter) |old_exporter| {
            old_exporter.deinit();
            old_exporter.destroy();
        }
        self.exporter = exporter;
    }

    /// Get the processor as a MetricProcessor union
    pub fn reader(self: *PeriodicReader) sdk.Reader {
        return .{ .bridge = sdk.BridgeReader.init(self) };
    }

    /// Background thread function that periodically collects metrics
    fn collectionThreadFn(self: *PeriodicReader) void {
        const interval: i96 = @as(i96, self.collection_interval_ms) * std.time.ns_per_ms;
        var next = nowNs() + interval;
        while (!self.stop.isSet()) {
            // The wait ends early when `stop` is set, and may end early for no reason:
            // only the clock says a tick is due.
            const left = next - nowNs();
            if (left > 0) {
                self.stop.waitTimeout(io, .{ .duration = .{ .raw = .fromNanoseconds(left), .clock = .awake } }) catch |err| switch (err) {
                    error.Timeout => {},
                    error.Canceled => break,
                };
                continue;
            }
            self.last_collection_time.store(@as(i64, @intCast(@divFloor(std.Io.Timestamp.now(io, .real).nanoseconds, std.time.ns_per_ms))), .release);
            _ = self.cycle(null);
            // a full interval after each cycle, however long its export took
            next = nowNs() + interval;
        }
        const limit = self.final_timeout_ms.load(.acquire);
        _ = self.cycle(if (limit == 0) null else limit);
    }
};

test "BasicPeriodicProcessor - direct init vs pipeline init thread behavior" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const MockExporter = @import("exporter.zig").MockMetricExporter;

    // Create mock exporter
    const mock_exporter = try allocator.create(MockExporter);
    mock_exporter.* = MockExporter.init(allocator);

    // Create a provider to tie all the parts together.
    var provider = sdk.MeterProvider.init(allocator, sdk.Resource.empty);
    defer provider.deinit();

    // Create processor with very short interval for testing (direct init)
    const processor = try allocator.create(PeriodicReader);
    {
        errdefer allocator.destroy(processor);
        processor.* = try PeriodicReader.init(allocator, mock_exporter.metricExporter(), 100); // 100ms
        {
            errdefer processor.deinit();
            try provider.registerReader(processor.reader());
        }
    }

    // Verify thread is not running when created via direct init()
    try testing.expect(!processor.is_running.load(.acquire));
    try testing.expect(processor.thread == null);

    // Create a basic meter for testing
    const scope = api.InstrumentationScope{ .name = "test.meter", .version = "1.0.0" };
    _ = try provider.getMeterWithScope(scope);

    // Start the thread manually (since we're not using pipeline)
    try processor.start();

    // Verify thread is now running
    try testing.expect(processor.is_running.load(.acquire));
    try testing.expect(processor.thread != null);

    // Create a second meter
    const scope2 = api.InstrumentationScope{ .name = "test.meter2", .version = "1.0.0" };
    _ = try provider.getMeterWithScope(scope2);

    // Verify thread is still running and we have both meters
    try testing.expect(processor.is_running.load(.acquire));
    try testing.expect(processor.thread != null);
    try testing.expect(processor.registered_meters.items.len == 2);

    // Wait a bit to allow some collections to happen
    io.sleep(.{ .nanoseconds = 250 * std.time.ns_per_ms }, .real) catch {};

    // Verify some exports happened (thread is working)
    const export_count = mock_exporter.exportCount();
    try testing.expect(export_count > 0);

    // Test shutdown - should stop the thread
    _ = processor.shutdown(1000);
    try testing.expect(processor.is_shutdown.load(.acquire));

    // Give thread time to exit
    io.sleep(.{ .nanoseconds = 50 * std.time.ns_per_ms }, .real) catch {};
}

test "BasicPeriodicProcessor - no thread start without meters" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Create processor
    var processor = try PeriodicReader.init(allocator, null, 100);
    defer processor.deinit();

    // Verify thread is not running
    try testing.expect(!processor.is_running.load(.acquire));
    try testing.expect(processor.thread == null);

    // Call forceFlush - should not crash
    const result = processor.forceFlush(100);
    try testing.expect(result == .success);

    // Thread should still not be running
    try testing.expect(!processor.is_running.load(.acquire));
    try testing.expect(processor.thread == null);

    // Shutdown should work fine
    _ = processor.shutdown(100);
    try testing.expect(processor.is_shutdown.load(.acquire));
}

test "PeriodicReader and Observable instrument test." {
    const testing = std.testing;
    const allocator = testing.allocator;
    const MockExporter = @import("exporter.zig").MockMetricExporter;

    // Create mock exporter
    const mock_exporter = try allocator.create(MockExporter);
    mock_exporter.* = MockExporter.init(allocator);

    // Create a provider to tie all the parts together.
    var provider = sdk.MeterProvider.init(allocator, sdk.Resource.empty);
    defer provider.deinit();

    // Create processor with very short interval for testing (direct init)
    const processor = try allocator.create(PeriodicReader);
    {
        errdefer allocator.destroy(processor);
        processor.* = try PeriodicReader.init(allocator, mock_exporter.metricExporter(), 100); // 100ms
        {
            errdefer processor.deinit();
            try provider.registerReader(processor.reader());
        }
    }

    try processor.start();

    const scope = api.InstrumentationScope{ .name = "cardinality", .version = "1.0.0" };
    var meter = try provider.getMeterWithScope(scope);
    const ctx = &[_]api.ContextKeyValue{};

    const CbStruct = struct {
        fn callback(alloc: std.mem.Allocator, result: *api.metrics.ObservableResult(i64), context: *anyopaque) void {
            const self: *PeriodicReader = @ptrCast(@alignCast(context));
            const cardinality = self.reader_state.aggregations.getCardinality();
            result.observeValue(alloc, @intCast(cardinality));
        }
    };

    const instrument = try meter.createObservableGauge(
        i64,
        "reader.cardinality",
        "how many active buckets in the reader aggregation.",
        "1",
        null,
        &[_]api.metrics.TypeErasedCallback(i64){},
    );

    _ = try instrument.registerCallback(PeriodicReader, CbStruct.callback, processor);

    const up_down = try meter.createCounter(i64, "foo", null, "1", null);
    for (0..15) |i| {
        const attributes = try api.AttributeBuilder.init(allocator)
            .add(.{ .key = "bar", .value = .{ .string = "baz" } })
            .add(.{ .key = "basic", .value = .{ .int = @intCast(i % 4) } })
            .finish(allocator);
        defer api.AttributeKeyValue.deinitOwnedSlice(allocator, attributes);
        up_down.add(ctx, 1, attributes);
        if (i % 12 == 0) {
            processor.collect();
        }
    }

    _ = provider.shutdown(null);

    var found = false;
    for (mock_exporter.exported_metrics.items) |value| {
        if (std.mem.eql(u8, value.name, instrument.getName())) {
            found = true;
        }
    }
    try testing.expect(found);
}

/// An exporter whose exports stay out: the first until `release_first` is set, the later
/// ones until `release_rest` is; each gives up at its timeout.
const GateExporter = struct {
    entered: std.Io.Event = .unset,
    release_first: std.Io.Event = .unset,
    release_rest: std.Io.Event = .unset,
    exports: std.atomic.Value(u32) = .init(0),
    /// The timeout the last export was given, in ms; maxInt: none.
    last_timeout_ms: std.atomic.Value(u64) = .init(0),

    pub fn exportMetrics(self: *GateExporter, metrics: []const sdk.MetricData) api.common.ExportResult {
        return self.exportMetricsWithin(metrics, null);
    }
    pub fn exportMetricsWithin(self: *GateExporter, _: []const sdk.MetricData, timeout_ms: ?u64) api.common.ExportResult {
        const release = if (self.exports.fetchAdd(1, .monotonic) == 0) &self.release_first else &self.release_rest;
        self.last_timeout_ms.store(timeout_ms orelse std.math.maxInt(u64), .monotonic);
        self.entered.set(io);
        if (timeout_ms) |ms| {
            release.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(@intCast(ms)), .clock = .awake } }) catch return .timeout;
        } else release.waitUncancelable(io);
        return .success;
    }
    pub fn forceFlush(_: *GateExporter, _: ?u64) api.common.ExportResult {
        return .success;
    }
    pub fn shutdown(_: *GateExporter, _: ?u64) api.common.ExportResult {
        return .success;
    }
    pub fn deinit(_: *GateExporter) void {}
    pub fn destroy(_: *GateExporter) void {}
    fn metricExporter(self: *GateExporter) sdk.MetricExporter {
        return .{ .bridge = @import("exporter.zig").BridgeMetricExporter.init(self) };
    }
};

fn elapsedMs(t0: i96) i96 {
    return @divFloor(std.Io.Timestamp.now(io, .awake).nanoseconds - t0, std.time.ns_per_ms);
}

test "fork: shutdown wakes the thread out of its interval and sends one last export" {
    const testing = std.testing;
    const MockExporter = @import("exporter.zig").MockMetricExporter;
    const mock_exporter = try testing.allocator.create(MockExporter);
    mock_exporter.* = MockExporter.init(testing.allocator);
    // a minute between collections: the old thread slept it out before it could be joined
    var reader_ = try PeriodicReader.init(testing.allocator, mock_exporter.metricExporter(), 60_000);
    try reader_.start();
    try testing.expectEqual(@as(u64, 0), mock_exporter.exportCount());

    const t0 = std.Io.Timestamp.now(io, .awake).nanoseconds;
    try testing.expectEqual(api.common.ProcessResult.success, reader_.shutdown(1000));
    try testing.expectEqual(@as(u64, 1), mock_exporter.exportCount());
    reader_.deinit();
    try testing.expect(elapsedMs(t0) < 1000);
}

test "fork: an export in flight holds neither the reader's mutex nor its shutdown past the timeout" {
    const testing = std.testing;
    var gate: GateExporter = .{};
    var reader_ = try PeriodicReader.init(testing.allocator, gate.metricExporter(), 20);
    try reader_.start();
    // the thread's first export is out, and stays out
    gate.entered.waitUncancelable(io);

    // what only needs the reader's own state does not wait for it
    var t0 = std.Io.Timestamp.now(io, .awake).nanoseconds;
    var no_meter: sdk.Meter = undefined;
    reader_.unregisterMeter(&no_meter);
    try testing.expect(elapsedMs(t0) < 200);
    try testing.expectEqual(@as(u32, 1), gate.exports.load(.monotonic));

    // shutdown: the export in flight is let go here (an exporter bounds its own), then the
    // last one is given the shutdown's timeout
    t0 = std.Io.Timestamp.now(io, .awake).nanoseconds;
    gate.release_first.set(io);
    _ = reader_.shutdown(150);
    const took = elapsedMs(t0);
    try testing.expectEqual(@as(u32, 2), gate.exports.load(.monotonic));
    try testing.expectEqual(@as(u64, 150), gate.last_timeout_ms.load(.monotonic));
    try testing.expect(took >= 100 and took < 1000);
    reader_.deinit();
}
