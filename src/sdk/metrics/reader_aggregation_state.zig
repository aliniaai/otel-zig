//! Reader Aggregation State for OpenTelemetry Metrics SDK
//!
//! This module manages aggregation state per reader, allowing multiple readers
//! to maintain independent aggregation states for the same instruments.
//!
//! Synchronous instruments aggregate into a map that every collection drains, so what they
//! export is the interval's own: a delta. Observable counters and histograms report running
//! totals; under delta temporality the state remembers each series' totals at the last
//! export that was committed and exports the difference. `commit` is the reader's word that
//! an export arrived: without it the next collection covers the missed interval too.

const std = @import("std");
const io = std.Options.debug_io;
const api = @import("otel-api");

const sdk = struct {
    const AttributeAggregationMap = @import("attribute_aggregation_map.zig").AttributeAggregationMap;
    const AttributeAggregationEntry = @import("attribute_aggregation_map.zig").AttributeAggregationEntry;
    const InstrumentType = @import("metadata.zig").InstrumentType;
    const MetricData = @import("data.zig").MetricData;
    const MetricDataPoint = @import("data.zig").MetricDataPoint;
    const MetricType = @import("data.zig").MetricType;
    const MetricDataValue = @import("data.zig").MetricValue;
    const MetricMetadata = @import("metadata.zig").MetricMetadata;
    const MetricValue = @import("reader.zig").MetricValue;
    const Resource = @import("../resource/resource.zig").Resource;
    const aggregations = @import("aggregations.zig");
};

pub const AggregationTemporality = sdk.aggregations.AggregationTemporality;
pub const AggregationType = sdk.aggregations.AggregationType;

/// Function type for selecting aggregation based on instrument type
pub const AggregationSelector = *const fn (instrument_type: sdk.InstrumentType) AggregationType;

/// Default aggregation selector based on instrument type
pub fn defaultAggregationSelector(instrument_type: sdk.InstrumentType) AggregationType {
    return switch (instrument_type) {
        .Counter, .UpDownCounter, .ObservableCounter, .ObservableUpDownCounter => .sum,
        .Gauge, .ObservableGauge => .last_value,
        .Histogram, .ObservableHistogram => .histogram,
    };
}

/// A series' running totals, as an observable instrument reported them.
const Totals = union(enum) {
    none,
    i64: i64,
    f64: f64,
    histogram: struct { counts: []u64, sum: f64 },

    fn deinit(t: *Totals, allocator: std.mem.Allocator) void {
        if (t.* == .histogram) allocator.free(t.histogram.counts);
        t.* = .none;
    }
};

/// What the state remembers of one observable series between collections.
const Observed = struct {
    /// At the last committed export.
    committed: Totals = .none,
    /// At the last collection, until it is committed.
    pending: Totals = .none,
    /// A histogram's extremes since the last committed export.
    min: ?f64 = null,
    max: ?f64 = null,
};

/// Per-reader aggregation state management
pub const ReaderAggregationState = struct {
    // Phase 1b: Attribute-based aggregation map with cardinality limits
    aggregations: sdk.AttributeAggregationMap,
    allocator: std.mem.Allocator,
    mutex: std.Io.Mutex,

    /// What observable counters and histograms are exported as. Synchronous instruments are
    /// always the interval's delta; observable up-down counters always the running total.
    temporality: AggregationTemporality,

    // Aggregation selector (determines aggregation type per instrument)
    aggregation_selector: AggregationSelector,

    /// When this state began: the start of every cumulative point.
    start_time_ns: u64,
    /// The last committed collection: the start of every observable delta.
    last_collection_time_ns: u64,
    /// The last collection, until it is committed.
    pending_collection_time_ns: u64,
    /// Observable series by the aggregation map's key. Under `mutex`.
    observed: std.AutoHashMapUnmanaged(u128, Observed),

    /// Initialize reader aggregation state
    pub fn init(
        allocator: std.mem.Allocator,
        temporality: AggregationTemporality,
        aggregation_selector: AggregationSelector,
    ) !@This() {
        const now: u64 = @intCast(std.Io.Timestamp.now(io, .real).nanoseconds);
        return .{
            .aggregations = try sdk.AttributeAggregationMap.init(allocator),
            .allocator = allocator,
            .mutex = std.Io.Mutex.init,
            .temporality = temporality,
            .aggregation_selector = aggregation_selector,
            .start_time_ns = now,
            .last_collection_time_ns = now,
            .pending_collection_time_ns = now,
            .observed = .empty,
        };
    }

    /// Clean up all aggregations and resources
    pub fn deinit(self: *@This()) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        // Clean up the attribute aggregation map
        self.aggregations.deinit();
        var it = self.observed.valueIterator();
        while (it.next()) |o| {
            o.committed.deinit(self.allocator);
            o.pending.deinit(self.allocator);
        }
        self.observed.deinit(self.allocator);
    }

    /// Record a measurement from an instrument (lock-free for aggregation updates)
    pub fn recordMeasurement(
        self: *@This(),
        value: sdk.MetricValue,
        attributes: []const api.AttributeKeyValue,
        metadata: sdk.MetricMetadata,
        metadata_hash: u64,
    ) void {
        // The map locks itself. An observed histogram's entry is created holding its point.
        const agg = self.aggregations.getOrCreateAggregation(attributes, metadata, metadata_hash, value);

        // Record the measurement lock-free (aggregations use atomic operations)
        switch (value) {
            .histogram => if (agg.aggregation != .observed_histogram) api.common.reportError(.{
                .component = .meter,
                .context = metadata.name,
                .error_type = .internal,
                .message = "Histogram observation dropped: its bucket count is not the instrument's, or the series limit is reached",
                .operation = "ReaderAggregationState.recordMeasurement",
                .source_error = null,
            }),
            inline .i64, .f64 => |v| switch (metadata.instrument_type) {
                .Counter, .UpDownCounter => agg.aggregation.add(v),
                else => _ = agg.aggregation.record(v),
            },
        }
    }

    /// The export of the last `collect` arrived: the next observable deltas start from it.
    pub fn commit(self: *@This()) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var it = self.observed.valueIterator();
        while (it.next()) |o| {
            if (o.pending == .none) continue;
            o.committed.deinit(self.allocator);
            o.committed = o.pending;
            o.pending = .none;
            o.min = null;
            o.max = null;
        }
        self.last_collection_time_ns = self.pending_collection_time_ns;
    }

    /// Collect metrics from all aggregations, tagged with `resource` (the provider's; not
    /// copied). Everything returned lives in `allocator`: an arena.
    pub fn collect(self: *@This(), allocator: std.mem.Allocator, resource: sdk.Resource) ![]sdk.MetricData {
        var entry_list = std.ArrayList(sdk.AttributeAggregationEntry).empty;
        defer entry_list.deinit(allocator);

        // Moves the entries out of the map, under its lock
        try self.aggregations.snapshot(allocator, &entry_list);

        var metrics_list = std.ArrayList(sdk.MetricData).empty;
        errdefer metrics_list.deinit(allocator);

        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const now = @as(u64, @intCast(std.Io.Timestamp.now(io, .real).nanoseconds));
        self.pending_collection_time_ns = now;

        for (entry_list.items) |*entry| {
            if (try self.metricOf(allocator, entry, now, resource)) |data| try metrics_list.append(allocator, data);
        }
        return metrics_list.toOwnedSlice(allocator);
    }

    /// The remembered totals of an observable series; null past the series limit.
    fn track(self: *@This(), key: u128) ?*Observed {
        if (self.observed.getPtr(key)) |o| return o;
        if (key == 0 or self.observed.count() >= sdk.AttributeAggregationMap.MAX_CARDINALITY) return null;
        const gop = self.observed.getOrPut(self.allocator, key) catch return null;
        gop.value_ptr.* = .{};
        return gop.value_ptr;
    }

    /// How much a running total grew; a total that went down restarted from zero.
    fn grown(comptime T: type, now: T, before: T) T {
        return if (now >= before) now - before else now;
    }

    /// Convert a single aggregation entry to MetricData; null when it has nothing to say.
    fn metricOf(
        self: *@This(),
        allocator: std.mem.Allocator,
        entry: *sdk.AttributeAggregationEntry,
        timestamp: u64,
        resource: sdk.Resource,
    ) !?sdk.MetricData {
        const kind = entry.metadata.instrument_type;
        var temporality: AggregationTemporality = .delta;
        var start: ?u64 = null;
        var metric_type: sdk.MetricType = .sum;
        const value: sdk.MetricDataValue = switch (entry.aggregation) {
            inline .sum_i64, .sum_f64 => |*sum, tag| blk: {
                const T = if (tag == .sum_i64) i64 else f64;
                var v: T = sum.value.load(.monotonic);
                start = sum.start_timestamp_ns;
                switch (kind) {
                    // the running total, as its callback reported it
                    .ObservableCounter => if (self.temporality == .delta) {
                        if (self.track(entry.key)) |o| {
                            const before: T = switch (o.committed) {
                                inline .i64, .f64 => |b| if (@TypeOf(b) == T) b else 0,
                                else => 0,
                            };
                            o.pending = if (T == i64) .{ .i64 = v } else .{ .f64 = v };
                            v = grown(T, v, before);
                            start = self.last_collection_time_ns;
                        } else {
                            temporality = .cumulative;
                            start = self.start_time_ns;
                        }
                    } else {
                        temporality = .cumulative;
                        start = self.start_time_ns;
                    },
                    .ObservableUpDownCounter => {
                        temporality = .cumulative;
                        start = self.start_time_ns;
                    },
                    else => {},
                }
                break :blk if (T == i64) .{ .i64_sum = v } else .{ .f64_sum = v };
            },
            inline .last_value_i64, .last_value_f64 => |*lv, tag| blk: {
                metric_type = .gauge;
                start = 0; // a last value has no start time
                const v = lv.getValue() orelse return null;
                break :blk if (tag == .last_value_i64) .{ .i64_gauge = v } else .{ .f64_gauge = v };
            },
            inline .histogram_i64, .histogram_f64 => |*hist, tag| blk: {
                metric_type = .histogram;
                if (hist.getCount() == 0) return null;
                start = hist.start_timestamp_ns;
                // Copy atomic bucket counts to regular u64 array
                const bucket_counts = try allocator.alloc(u64, hist.counts.len);
                for (hist.counts, bucket_counts) |*atomic_count, *c| c.* = atomic_count.load(.monotonic);
                break :blk @unionInit(sdk.MetricDataValue, if (tag == .histogram_i64) "i64_histogram" else "f64_histogram", .{
                    .count = hist.getCount(),
                    .sum = hist.getSum(),
                    .min = hist.getMin(),
                    .max = hist.getMax(),
                    .boundaries = hist.boundaries,
                    .bucket_counts = bucket_counts,
                });
            },
            .observed_histogram => |*hist| blk: {
                metric_type = .histogram;
                var count: u64 = 0;
                var sum = hist.sum;
                var min = hist.min;
                var max = hist.max;
                const tracked = if (self.temporality == .delta) self.track(entry.key) else null;
                if (tracked) |o| {
                    // the extremes of every collection since the last committed export
                    if (hist.min) |m| o.min = if (o.min) |x| @min(x, m) else m;
                    if (hist.max) |m| o.max = if (o.max) |x| @max(x, m) else m;
                    min = o.min;
                    max = o.max;
                    const totals = try self.allocator.dupe(u64, hist.bucket_counts);
                    o.pending.deinit(self.allocator);
                    o.pending = .{ .histogram = .{ .counts = totals, .sum = hist.sum } };
                    if (o.committed == .histogram and o.committed.histogram.counts.len == totals.len) {
                        const before = o.committed.histogram;
                        var restarted = false;
                        for (totals, before.counts) |t, b| restarted = restarted or t < b;
                        if (!restarted) {
                            for (hist.bucket_counts, before.counts) |*c, b| c.* -= b;
                            sum -= before.sum;
                        }
                    }
                    start = self.last_collection_time_ns;
                } else {
                    temporality = .cumulative;
                    start = self.start_time_ns;
                }
                for (hist.bucket_counts) |c| count += c;
                if (count == 0) return null;
                break :blk .{ .f64_histogram = .{
                    .count = count,
                    .sum = sum,
                    .min = min,
                    .max = max,
                    .boundaries = hist.boundaries,
                    .bucket_counts = hist.bucket_counts,
                } };
            },
            // Drop aggregation produces no metric data
            .drop => return null,
        };

        const data_points = try allocator.alloc(sdk.MetricDataPoint, 1);
        data_points[0] = .{
            .timestamp_ns = timestamp,
            .start_timestamp_ns = start,
            .attributes = entry.attributes,
            .value = value,
        };
        return .{
            .name = entry.metadata.name,
            .description = if (entry.metadata.description.len > 0) entry.metadata.description else null,
            .unit = if (entry.metadata.unit.len > 0) entry.metadata.unit else null,
            .type = metric_type,
            .data_points = data_points,
            .scope = entry.metadata.instrumentation_scope,
            .resource = resource,
            .temporality = temporality,
            .is_monotonic = metric_type == .sum and (kind == .Counter or kind == .ObservableCounter),
        };
    }
};
