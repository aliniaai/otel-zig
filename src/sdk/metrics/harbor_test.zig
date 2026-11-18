//! What harbor's fork changed in the metrics SDK, through a provider, a ManualReader and the
//! mock exporter: what an exporter is handed, collection by collection.
const std = @import("std");
const api = @import("otel-api");
const testing = std.testing;

const MeterProvider = @import("meter_provider.zig").MeterProvider;
const ManualReader = @import("manual_reader.zig").ManualReader;
const MockMetricExporter = @import("exporter.zig").MockMetricExporter;
const MetricData = @import("data.zig").MetricData;
const Resource = @import("../resource/resource.zig").Resource;
const HistogramPoint = api.metrics.HistogramPoint;

const Rig = struct {
    provider: MeterProvider,
    reader: *ManualReader,
    exporter: *MockMetricExporter,
    meter: api.metrics.Meter,

    /// In place: the meters keep a pointer to the provider.
    fn init(rig: *Rig) !void {
        const allocator = testing.allocator;
        var attrs = api.common.AttributeBuilder.init(allocator);
        attrs = attrs.add(.{ .key = "service.name", .value = .{ .string = "svc" } });
        rig.provider = MeterProvider.init(allocator, try Resource.initOwnedFromBuilder(allocator, null, &attrs));
        rig.provider.async_config = .production;
        errdefer rig.provider.deinit();
        rig.exporter = try allocator.create(MockMetricExporter);
        rig.exporter.* = MockMetricExporter.init(allocator);
        rig.reader = try allocator.create(ManualReader);
        rig.reader.* = try ManualReader.init(allocator, rig.exporter.metricExporter());
        try rig.provider.registerReader(rig.reader.reader());
        rig.meter = try rig.provider.getMeterWithScope(.{ .name = "test" });
    }

    fn deinit(rig: *Rig) void {
        rig.provider.deinit();
    }

    /// Collects, and returns what that one collection exported under `name`.
    fn collect(rig: *Rig, buffer: []MetricData, name: []const u8) []MetricData {
        rig.exporter.deinit();
        rig.exporter.exported_metrics = .empty;
        rig.reader.collect();
        return rig.exporter.getMetricsNamed(buffer, name);
    }
};

fn observeTotal(allocator: std.mem.Allocator, result: *api.metrics.ObservableResult(i64), state: *anyopaque) void {
    const v: *const i64 = @ptrCast(@alignCast(state));
    result.observeValue(allocator, v.*);
}

test "fork: an observable counter is exported as the growth of its running total, monotonic" {
    var rig: Rig = undefined;
    try rig.init();
    defer rig.deinit();
    var value: i64 = 10;
    const counter = try rig.meter.createObservableCounter(i64, "requests", null, "1", null, &.{});
    _ = try counter.registerCallback(i64, observeTotal, &value);

    var buf: [4]MetricData = undefined;
    var got = rig.collect(&buf, "requests");
    try testing.expectEqual(@as(usize, 1), got.len);
    try testing.expectEqual(@as(i64, 10), got[0].data_points[0].value.i64_sum);
    try testing.expectEqual(MetricData.Temporality.delta, got[0].temporality);
    try testing.expect(got[0].is_monotonic);
    const first_time = got[0].data_points[0].timestamp_ns;

    value = 25;
    got = rig.collect(&buf, "requests");
    try testing.expectEqual(@as(i64, 15), got[0].data_points[0].value.i64_sum);
    // the delta starts where the previous one ended
    try testing.expectEqual(first_time, got[0].data_points[0].start_timestamp_ns.?);

    // nothing new: a zero, not the total again
    got = rig.collect(&buf, "requests");
    try testing.expectEqual(@as(i64, 0), got[0].data_points[0].value.i64_sum);

    // a total that went down restarted from zero
    value = 4;
    got = rig.collect(&buf, "requests");
    try testing.expectEqual(@as(i64, 4), got[0].data_points[0].value.i64_sum);
}

test "fork: an export that fails is not committed; the next delta covers both intervals" {
    var rig: Rig = undefined;
    try rig.init();
    defer rig.deinit();
    var value: i64 = 10;
    const counter = try rig.meter.createObservableCounter(i64, "requests", null, "1", null, &.{});
    _ = try counter.registerCallback(i64, observeTotal, &value);

    var buf: [4]MetricData = undefined;
    var got = rig.collect(&buf, "requests");
    const committed_at = got[0].data_points[0].timestamp_ns;

    rig.exporter.export_result = .failure;
    value = 30;
    got = rig.collect(&buf, "requests");
    try testing.expectEqual(@as(i64, 20), got[0].data_points[0].value.i64_sum);

    rig.exporter.export_result = .success;
    value = 42;
    got = rig.collect(&buf, "requests");
    try testing.expectEqual(@as(i64, 32), got[0].data_points[0].value.i64_sum);
    try testing.expectEqual(committed_at, got[0].data_points[0].start_timestamp_ns.?);

    value = 43;
    got = rig.collect(&buf, "requests");
    try testing.expectEqual(@as(i64, 1), got[0].data_points[0].value.i64_sum);
}

test "fork: every metric carries the provider's resource" {
    var rig: Rig = undefined;
    try rig.init();
    defer rig.deinit();
    const counter = try rig.meter.createCounter(i64, "hits", null, "1", null);
    counter.add(&.{}, 3, &.{});
    var buf: [4]MetricData = undefined;
    const got = rig.collect(&buf, "hits");
    try testing.expectEqual(@as(usize, 1), got.len);
    const name = api.AttributeKeyValue.scanSlice(got[0].resource.attributes, "service.name").?;
    try testing.expectEqualStrings("svc", name.value.string);
}

test "fork: synchronous sums are the interval's own; an up-down counter is not monotonic" {
    var rig: Rig = undefined;
    try rig.init();
    defer rig.deinit();
    const counter = try rig.meter.createCounter(i64, "hits", null, "1", null);
    const level = try rig.meter.createUpDownCounter(i64, "level", null, "1", null);
    var value: i64 = 7;
    const observed_level = try rig.meter.createObservableUpDownCounter(i64, "observed_level", null, "1", null, &.{});
    _ = try observed_level.registerCallback(i64, observeTotal, &value);

    counter.add(&.{}, 3, &.{});
    level.add(&.{}, -2, &.{});
    var buf: [4]MetricData = undefined;
    var got = rig.collect(&buf, "hits");
    try testing.expectEqual(@as(i64, 3), got[0].data_points[0].value.i64_sum);
    try testing.expect(got[0].is_monotonic);
    try testing.expectEqual(MetricData.Temporality.delta, got[0].temporality);

    counter.add(&.{}, 2, &.{});
    got = rig.collect(&buf, "hits");
    try testing.expectEqual(@as(i64, 2), got[0].data_points[0].value.i64_sum);

    level.add(&.{}, 5, &.{});
    got = rig.collect(&buf, "level");
    try testing.expect(!got[0].is_monotonic);
    // an observable up-down counter stays a running total
    got = rig.collect(&buf, "observed_level");
    try testing.expectEqual(@as(i64, 7), got[0].data_points[0].value.i64_sum);
    try testing.expectEqual(MetricData.Temporality.cumulative, got[0].temporality);
    try testing.expect(!got[0].is_monotonic);
}

const Buckets = struct {
    counts: [4]u64 = .{ 0, 0, 0, 0 },
    sum: f64 = 0,
    min: ?f64 = null,
    max: ?f64 = null,
    /// What the callback reports; 4 is the instrument's.
    reported: usize = 4,

    fn observe(allocator: std.mem.Allocator, result: *api.metrics.ObservableResult(HistogramPoint), state: *anyopaque) void {
        const b: *const Buckets = @ptrCast(@alignCast(state));
        const counts = allocator.dupe(u64, b.counts[0..b.reported]) catch return;
        result.observeValue(allocator, .{ .bucket_counts = counts, .sum = b.sum, .min = b.min, .max = b.max });
    }
};

test "fork: an observable histogram is exported as the growth of its bucket totals" {
    var rig: Rig = undefined;
    try rig.init();
    defer rig.deinit();
    var b: Buckets = .{};
    const boundaries = [_]f64{ 1, 10, 100 };
    const hist = try rig.meter.createObservableHistogram("latency", "request latency", "ms", .{ .explicit_bucket_boundaries = &boundaries }, &.{});
    _ = try hist.registerCallback(Buckets, Buckets.observe, &b);
    try testing.expectError(error.NoBucketBoundaries, rig.meter.createObservableHistogram("no_bounds", null, "ms", .{}, &.{}));

    // nothing recorded: no point
    var buf: [4]MetricData = undefined;
    var got = rig.collect(&buf, "latency");
    try testing.expectEqual(@as(usize, 0), got.len);

    b = .{ .counts = .{ 1, 2, 0, 1 }, .sum = 520.5, .min = 0.5, .max = 500 };
    got = rig.collect(&buf, "latency");
    try testing.expectEqual(@as(usize, 1), got.len);
    var h = got[0].data_points[0].value.f64_histogram;
    try testing.expectEqual(@as(u64, 4), h.count);
    try testing.expectEqualSlices(u64, &.{ 1, 2, 0, 1 }, h.bucket_counts);
    try testing.expectEqualSlices(f64, &boundaries, h.boundaries);
    try testing.expectEqual(@as(f64, 520.5), h.sum);
    try testing.expectEqual(@as(?f64, 0.5), h.min);
    try testing.expectEqual(@as(?f64, 500), h.max);
    try testing.expectEqual(MetricData.Temporality.delta, got[0].temporality);
    try testing.expect(got[0].type == .histogram);
    try testing.expectEqualStrings("ms", got[0].unit.?);

    // two more in the second bucket: the growth, and this interval's extremes only
    b = .{ .counts = .{ 1, 4, 0, 1 }, .sum = 530.5, .min = 4, .max = 6 };
    got = rig.collect(&buf, "latency");
    h = got[0].data_points[0].value.f64_histogram;
    try testing.expectEqual(@as(u64, 2), h.count);
    try testing.expectEqualSlices(u64, &.{ 0, 2, 0, 0 }, h.bucket_counts);
    try testing.expectApproxEqAbs(@as(f64, 10), h.sum, 1e-9);
    try testing.expectEqual(@as(?f64, 4), h.min);
    try testing.expectEqual(@as(?f64, 6), h.max);

    // no growth: no point
    got = rig.collect(&buf, "latency");
    try testing.expectEqual(@as(usize, 0), got.len);

    // a failed export: the next point covers both intervals, extremes included
    rig.exporter.export_result = .failure;
    b = .{ .counts = .{ 2, 4, 0, 1 }, .sum = 531, .min = 0.5, .max = 0.5 };
    got = rig.collect(&buf, "latency");
    try testing.expectEqual(@as(u64, 1), got[0].data_points[0].value.f64_histogram.count);
    rig.exporter.export_result = .success;
    b = .{ .counts = .{ 2, 4, 3, 1 }, .sum = 681, .min = 50, .max = 50 };
    got = rig.collect(&buf, "latency");
    h = got[0].data_points[0].value.f64_histogram;
    try testing.expectEqualSlices(u64, &.{ 1, 0, 3, 0 }, h.bucket_counts);
    try testing.expectApproxEqAbs(@as(f64, 150.5), h.sum, 1e-9);
    try testing.expectEqual(@as(?f64, 0.5), h.min);
    try testing.expectEqual(@as(?f64, 50), h.max);

    // a point with another number of buckets is dropped
    b.reported = 3;
    got = rig.collect(&buf, "latency");
    try testing.expectEqual(@as(usize, 0), got.len);
}

const Order = struct {
    /// Room for two collections: the test's, and the reader's last one at shutdown.
    var seen: [6]u8 = undefined;
    var n: usize = 0;

    fn note(c: u8) void {
        seen[n] = c;
        n += 1;
    }
    fn histogram(_: std.mem.Allocator, _: *api.metrics.ObservableResult(HistogramPoint)) void {
        note('h');
    }
    fn int(_: std.mem.Allocator, _: *api.metrics.ObservableResult(i64)) void {
        note('i');
    }
    fn float(_: std.mem.Allocator, _: *api.metrics.ObservableResult(f64)) void {
        note('f');
    }
};

test "fork: callbacks run histograms first, then i64, then f64 instruments" {
    var rig: Rig = undefined;
    try rig.init();
    defer rig.deinit();
    // created in the opposite order
    const f = try rig.meter.createObservableGauge(f64, "f", null, "1", null, &.{});
    _ = try f.registerCallbackNoState(Order.float);
    const i = try rig.meter.createObservableGauge(i64, "i", null, "1", null, &.{});
    _ = try i.registerCallbackNoState(Order.int);
    const h = try rig.meter.createObservableHistogram("h", null, "1", .{ .explicit_bucket_boundaries = &.{1} }, &.{});
    _ = try h.registerCallbackNoState(Order.histogram);
    Order.n = 0;
    rig.reader.collect();
    try testing.expectEqualStrings("hif", Order.seen[0..Order.n]);
}

test "fork: many collections of observable instruments leak nothing" {
    var rig: Rig = undefined;
    try rig.init();
    defer rig.deinit();
    var value: i64 = 0;
    var b: Buckets = .{};
    const counter = try rig.meter.createObservableCounter(i64, "requests", null, "1", null, &.{});
    _ = try counter.registerCallback(i64, observeTotal, &value);
    const hist = try rig.meter.createObservableHistogram("latency", null, "ms", .{ .explicit_bucket_boundaries = &.{ 1, 10, 100 } }, &.{});
    _ = try hist.registerCallback(Buckets, Buckets.observe, &b);
    for (0..500) |round| {
        value += 3;
        b.counts[round % 4] += 1;
        b.sum += 1;
        rig.exporter.export_result = if (round % 7 == 0) .failure else .success;
        rig.exporter.deinit();
        rig.exporter.exported_metrics = .empty;
        rig.reader.collect();
    }
    try testing.expectEqual(@as(usize, 2), rig.reader.reader_state.observed.count());
}

test "fork: histogram min/max and last value use 64-bit atomics, empty is null" {
    const agg = @import("aggregations.zig");
    inline for (.{ i64, f64 }) |T| {
        var h = try agg.HistogramAggregation(T).init(testing.allocator, .{});
        defer h.deinit(testing.allocator);
        try testing.expectEqual(@as(?T, null), h.getMin());
        try testing.expectEqual(@as(?T, null), h.getMax());

        // concurrent recorders: min and max are the extremes of everything recorded
        const Rec = struct {
            fn run(hist: *agg.HistogramAggregation(T), base: i64) void {
                var i: i64 = 0;
                while (i < 1000) : (i += 1) {
                    const v = base + @mod(i * 37, 1000) - 500;
                    hist.record(if (T == f64) @as(f64, @floatFromInt(v)) / 4 else v);
                }
            }
        };
        var threads: [4]std.Thread = undefined;
        for (&threads, 0..) |*t, k| t.* = try std.Thread.spawn(.{}, Rec.run, .{ &h, @as(i64, @intCast(k)) * 10 });
        for (threads) |t| t.join();
        try testing.expectEqual(@as(u64, 4000), h.getCount());
        const lo: T = if (T == f64) -500.0 / 4.0 else -500;
        const hi: T = if (T == f64) 529.0 / 4.0 else 529;
        try testing.expectEqual(@as(?T, lo), h.getMin());
        try testing.expectEqual(@as(?T, hi), h.getMax());

        h.reset();
        try testing.expectEqual(@as(?T, null), h.getMin());
        h.record(if (T == f64) 2.5 else 3);
        try testing.expectEqual(@as(?T, if (T == f64) 2.5 else 3), h.getMin());
        try testing.expectEqual(@as(?T, if (T == f64) 2.5 else 3), h.getMax());

        var lv = agg.LastValueAggregation(T).init();
        try testing.expectEqual(@as(?T, null), lv.getValue());
        lv.record(if (T == f64) -1.5 else -7);
        try testing.expectEqual(@as(?T, if (T == f64) -1.5 else -7), lv.getValue());
        lv.reset();
        try testing.expectEqual(@as(?T, null), lv.getValue());
    }
}
