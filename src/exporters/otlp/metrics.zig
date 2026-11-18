const std = @import("std");
const io = std.Options.debug_io;
const otel_api = @import("otel-api");
const otel_sdk = @import("otel-sdk");
const protobuf = @import("protobuf");

const ExportResult = otel_api.common.ExportResult;
const OtlpExporterConfig = @import("root.zig").OtlpExporterConfig;
const MetricData = otel_sdk.metrics.MetricData;
const MetricDataPoint = otel_sdk.metrics.MetricDataPoint;
const MetricType = otel_sdk.metrics.MetricType;
const MetricExporter = otel_sdk.metrics.MetricExporter;
const convert = @import("convert.zig");
const transport = @import("transport.zig");

// Import error handler for structured error reporting
const error_handler = otel_api.common;

// Import protobuf definitions
const metrics_v1 = @import("proto/opentelemetry/proto/metrics/v1.pb.zig");
const common_v1 = @import("proto/opentelemetry/proto/common/v1.pb.zig");
const resource_v1 = @import("proto/opentelemetry/proto/resource/v1.pb.zig");

/// Exports metrics as OTLP/HTTP protobuf through the shared transport: every POST has the
/// config's timeout, retries stay inside its export budget, and nothing is held while the
/// request is out. A reader calls it from one thread at a time.
pub const OtlpMetricExporter = struct {
    pub const PipelineStep = otel_sdk.common.PipelineStepInstructions(
        Self,
        otel_sdk.metrics.MetricExporter,
        OtlpExporterConfig,
        metricsExporter,
        _init,
        otel_sdk.common.PipelineDeinitConnection,
    );
    const Self = @This();

    pub fn _init(self: *Self, ctx: OtlpExporterConfig, allocator: std.mem.Allocator) !void {
        self.* = init(allocator, ctx);
    }

    config: OtlpExporterConfig,
    allocator: std.mem.Allocator,
    is_shutdown: std.atomic.Value(bool) = .init(false),
    /// The last export failed: an outage is reported once, not once per export.
    failing: std.atomic.Value(bool) = .init(false),
    exports_ok: std.atomic.Value(u64) = .init(0),
    exports_failed: std.atomic.Value(u64) = .init(0),
    last_export_ns: std.atomic.Value(u64) = .init(0),
    last_export_bytes: std.atomic.Value(u64) = .init(0),

    /// What the exporter has done so far, for an application that exports it as metrics.
    pub const Stats = struct {
        exports_ok: u64,
        exports_failed: u64,
        /// The last export: its POST(s), start to answer, and its body.
        last_export_ns: u64,
        last_export_bytes: u64,
    };

    pub fn init(allocator: std.mem.Allocator, config: OtlpExporterConfig) OtlpMetricExporter {
        return .{
            .config = config,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *OtlpMetricExporter) void {
        _ = self;
    }

    pub fn destroy(self: *OtlpMetricExporter) void {
        self.allocator.destroy(self);
    }

    pub fn stats(self: *const OtlpMetricExporter) Stats {
        return .{
            .exports_ok = self.exports_ok.load(.monotonic),
            .exports_failed = self.exports_failed.load(.monotonic),
            .last_export_ns = self.last_export_ns.load(.monotonic),
            .last_export_bytes = self.last_export_bytes.load(.monotonic),
        };
    }

    pub fn exportMetrics(self: *OtlpMetricExporter, metrics: []const MetricData) ExportResult {
        return self.exportMetricsWithin(metrics, null);
    }

    /// `exportMetrics` with the config's timeouts cut to `timeout_ms` (a reader's shutdown
    /// budget).
    pub fn exportMetricsWithin(self: *OtlpMetricExporter, metrics: []const MetricData, timeout_ms: ?u64) ExportResult {
        if (self.is_shutdown.load(.acquire)) {
            return .failure;
        }
        if (metrics.len == 0) return .success;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const result = self.send(allocator, metrics, timeout_ms) catch |err| {
            self.failed(.{
                .component = .exporter,
                .operation = "otlp_metric_export",
                .error_type = if (err == error.OutOfMemory) .serialization else .network,
                .message = "OTLP metrics export failed (not reported again until one succeeds)",
                .context = self.config.endpoint,
                .source_error = err,
            });
            return .failure;
        };
        if (result != .success) {
            // an HTTP error status: the transport reported it
            _ = self.exports_failed.fetchAdd(1, .monotonic);
            self.failing.store(true, .release);
            return result;
        }
        _ = self.exports_ok.fetchAdd(1, .monotonic);
        self.failing.store(false, .release);
        return .success;
    }

    fn failed(self: *OtlpMetricExporter, info: error_handler.ErrorInfo) void {
        _ = self.exports_failed.fetchAdd(1, .monotonic);
        if (!self.failing.swap(true, .acq_rel)) error_handler.reportError(info);
    }

    fn send(self: *OtlpMetricExporter, allocator: std.mem.Allocator, metrics: []const MetricData, timeout_ms: ?u64) !ExportResult {
        const metrics_data = try convertToOtlpFormat(allocator, metrics);
        var buffer = std.Io.Writer.Allocating.init(allocator);
        metrics_data.encode(&buffer.writer, allocator) catch return error.OutOfMemory;
        const payload = buffer.written();
        self.last_export_bytes.store(payload.len, .monotonic);

        var config = self.config;
        if (timeout_ms) |limit| {
            config.timeout_millis = @min(config.timeout_millis, limit);
            config.export_timeout_millis = @min(config.export_timeout_millis, limit);
        }
        const url = try transport.signalUrl(allocator, config, config.protocol_config.metrics_path);
        const started = std.Io.Timestamp.now(config.io, .awake).nanoseconds;
        defer self.last_export_ns.store(@intCast(@max(std.Io.Timestamp.now(config.io, .awake).nanoseconds - started, 0)), .monotonic);
        return transport.post(self.allocator, allocator, config, url, "application/x-protobuf", config.headers, payload, "metrics");
    }

    pub fn forceFlush(self: *OtlpMetricExporter, timeout_ms: ?u64) ExportResult {
        _ = self;
        _ = timeout_ms;
        // For HTTP-based exporters, force flush is typically a no-op
        // since each export call is synchronous
        return .success;
    }

    pub fn shutdown(self: *OtlpMetricExporter, timeout_ms: ?u64) ExportResult {
        _ = timeout_ms;
        self.is_shutdown.store(true, .release);
        return .success;
    }

    pub fn metricsExporter(self: *OtlpMetricExporter) MetricExporter {
        return .{
            .bridge = otel_sdk.metrics.BridgeMetricExporter.init(self),
        };
    }
};

/// Create a console metric exporter with custom configuration
pub fn createMetricExporterWithConfig(config: OtlpExporterConfig, allocator: std.mem.Allocator) !otel_sdk.metrics.MetricExporter {
    const exporter = try allocator.create(OtlpMetricExporter);
    errdefer allocator.destroy(exporter);
    exporter.* = OtlpMetricExporter.init(allocator, config);
    return exporter.metricsExporter();
}

fn temporalityToProto(t: otel_sdk.metrics.MetricData.Temporality) metrics_v1.AggregationTemporality {
    return switch (t) {
        .delta => .AGGREGATION_TEMPORALITY_DELTA,
        .cumulative => .AGGREGATION_TEMPORALITY_CUMULATIVE,
    };
}

/// `allocator` is an arena: the returned messages are freed with it.
fn convertToOtlpFormat(allocator: std.mem.Allocator, metrics: []const MetricData) !metrics_v1.MetricsData {
    var resource_metrics_map = std.StringHashMap(std.ArrayList(MetricData)).init(allocator);
    defer {
        var it = resource_metrics_map.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(allocator);
        }
        resource_metrics_map.deinit();
    }

    // Group metrics by resource
    for (metrics) |metric| {
        // Group by service name if available, otherwise use empty string
        const resource_key = if (otel_api.AttributeKeyValue.scanSlice(metric.resource.attributes, "service.name")) |attr|
            attr.value.string
        else
            "";
        const result = try resource_metrics_map.getOrPut(resource_key);
        if (!result.found_existing) {
            result.value_ptr.* = std.ArrayList(MetricData).empty;
        }
        try result.value_ptr.append(allocator, metric);
    }

    var metrics_data = metrics_v1.MetricsData{};

    var resource_it = resource_metrics_map.iterator();
    while (resource_it.next()) |entry| {
        const resource_metrics = entry.value_ptr.*;
        if (resource_metrics.items.len == 0) continue;

        // Convert resource
        var rm = metrics_v1.ResourceMetrics{
            .resource = try convert.resourceToProto(allocator, resource_metrics.items[0].resource),
        };

        // Group by instrumentation scope
        var scope_map = std.StringHashMap(std.ArrayList(MetricData)).init(allocator);
        defer {
            var it = scope_map.iterator();
            while (it.next()) |scope_entry| {
                scope_entry.value_ptr.deinit(allocator);
            }
            scope_map.deinit();
        }

        for (resource_metrics.items) |metric| {
            const scope_name = metric.scope.name;
            const result = try scope_map.getOrPut(scope_name);
            if (!result.found_existing) {
                result.value_ptr.* = std.ArrayList(MetricData).empty;
            }
            try result.value_ptr.append(allocator, metric);
        }

        var scope_it = scope_map.iterator();
        while (scope_it.next()) |scope_entry| {
            const scope_metrics = scope_entry.value_ptr.*;
            if (scope_metrics.items.len == 0) continue;

            // Convert instrumentation scope
            var sm = metrics_v1.ScopeMetrics{
                .scope = try convert.instrumentationScopeToProto(allocator, scope_metrics.items[0].scope),
            };

            // A reader hands over one MetricData per attribute set: the points of one
            // instrument go into one Metric, by name (and the rest of what a Metric fixes).
            var by_name = std.StringHashMap(usize).init(allocator);
            defer by_name.deinit();

            // Convert metrics
            for (scope_metrics.items) |metric_data| {
                const slot = try by_name.getOrPut(metric_data.name);
                if (slot.found_existing) {
                    const existing = &sm.metrics.items[slot.value_ptr.*];
                    const merged = switch (existing.data.?) {
                        .sum => |*sum| metric_data.type == .sum and sum.aggregation_temporality == temporalityToProto(metric_data.temporality) and sum.is_monotonic == metric_data.is_monotonic and blk: {
                            var points = try convertNumberDataPoints(allocator, metric_data.data_points);
                            try sum.data_points.appendSlice(allocator, points.items);
                            points.deinit(allocator);
                            break :blk true;
                        },
                        .gauge => |*gauge| metric_data.type == .gauge and blk: {
                            var points = try convertNumberDataPoints(allocator, metric_data.data_points);
                            try gauge.data_points.appendSlice(allocator, points.items);
                            points.deinit(allocator);
                            break :blk true;
                        },
                        .histogram => |*histogram| metric_data.type == .histogram and histogram.aggregation_temporality == temporalityToProto(metric_data.temporality) and blk: {
                            var points = try convertHistogramDataPoints(allocator, metric_data.data_points);
                            try histogram.data_points.appendSlice(allocator, points.items);
                            points.deinit(allocator);
                            break :blk true;
                        },
                        else => false,
                    };
                    if (merged) continue;
                } else slot.value_ptr.* = sm.metrics.items.len;

                var metric = metrics_v1.Metric{
                    .name = try allocator.dupe(u8, metric_data.name),
                    .description = try allocator.dupe(u8, metric_data.description orelse ""),
                    .unit = try allocator.dupe(u8, metric_data.unit orelse ""),
                };

                // Convert metric data based on type
                switch (metric_data.type) {
                    .sum => {
                        metric.data = .{ .sum = .{
                            .aggregation_temporality = temporalityToProto(metric_data.temporality),
                            .is_monotonic = metric_data.is_monotonic,
                            .data_points = try convertNumberDataPoints(allocator, metric_data.data_points),
                        } };
                    },
                    .gauge => {
                        metric.data = .{ .gauge = .{
                            .data_points = try convertNumberDataPoints(allocator, metric_data.data_points),
                        } };
                    },
                    .histogram => {
                        metric.data = .{ .histogram = .{
                            .aggregation_temporality = temporalityToProto(metric_data.temporality),
                            .data_points = try convertHistogramDataPoints(allocator, metric_data.data_points),
                        } };
                    },
                }

                try sm.metrics.append(allocator, metric);
            }

            try rm.scope_metrics.append(allocator, sm);
        }

        try metrics_data.resource_metrics.append(allocator, rm);
    }

    return metrics_data;
}

fn convertNumberDataPoints(allocator: std.mem.Allocator, data_points: []const MetricDataPoint) !std.ArrayList(metrics_v1.NumberDataPoint) {
    var result = std.ArrayList(metrics_v1.NumberDataPoint).empty;

    for (data_points) |dp| {
        var ndp = metrics_v1.NumberDataPoint{
            .time_unix_nano = dp.timestamp_ns,
            .start_time_unix_nano = dp.start_timestamp_ns orelse 0,
        };

        // Convert attributes
        for (dp.attributes) |attr| {
            try ndp.attributes.append(allocator, try convert.attributeKeyValueToProto(allocator, attr));
        }

        // Set value based on type
        switch (dp.value) {
            .i64_gauge => |i| ndp.value = .{ .as_int = i },
            .f64_gauge => |d| ndp.value = .{ .as_double = d },
            .i64_sum => |i| ndp.value = .{ .as_int = i },
            .f64_sum => |d| ndp.value = .{ .as_double = d },
            .i64_histogram, .f64_histogram => unreachable, // Histogram data points are handled separately
        }

        try result.append(allocator, ndp);
    }

    return result;
}

fn convertHistogramDataPoints(allocator: std.mem.Allocator, data_points: []const MetricDataPoint) !std.ArrayList(metrics_v1.HistogramDataPoint) {
    var result = std.ArrayList(metrics_v1.HistogramDataPoint).empty;

    for (data_points) |dp| {
        var hdp = metrics_v1.HistogramDataPoint{
            .time_unix_nano = dp.timestamp_ns,
            .start_time_unix_nano = dp.start_timestamp_ns orelse 0,
        };

        // Convert attributes
        for (dp.attributes) |attr| {
            try hdp.attributes.append(allocator, try convert.attributeKeyValueToProto(allocator, attr));
        }

        switch (dp.value) {
            .i64_histogram => |h| {
                hdp.count = h.count;
                hdp.sum = @floatFromInt(h.sum);

                for (h.bucket_counts) |count| {
                    try hdp.bucket_counts.append(allocator, count);
                }

                for (h.boundaries) |bound| {
                    try hdp.explicit_bounds.append(allocator, bound);
                }

                hdp.min = if (h.min) |min| @floatFromInt(min) else null;
                hdp.max = if (h.max) |max| @floatFromInt(max) else null;
            },
            .f64_histogram => |h| {
                hdp.count = h.count;
                hdp.sum = h.sum;

                for (h.bucket_counts) |count| {
                    try hdp.bucket_counts.append(allocator, count);
                }

                for (h.boundaries) |bound| {
                    try hdp.explicit_bounds.append(allocator, bound);
                }

                hdp.min = h.min;
                hdp.max = h.max;
            },
            else => unreachable, // Only histogram data points should be passed here
        }

        try result.append(allocator, hdp);
    }

    return result;
}

test "convertToOtlpFormat with histogram" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Create test resource
    const resource_attrs = [_]otel_api.common.AttributeKeyValue{
        .{ .key = "service.name", .value = .{ .string = "test-service" } },
    };
    const resource = otel_sdk.resource.Resource{ .attributes = &resource_attrs };

    // Create test instrumentation scope
    const scope = otel_api.InstrumentationScope{
        .name = "test-scope",
        .version = "1.0.0",
    };

    // Create test histogram data
    const boundaries = [_]f64{ 1.0, 5.0, 10.0 };
    const bucket_counts = [_]u64{ 2, 3, 1, 0 };
    const histogram = otel_sdk.metrics.F64HistogramData{
        .count = 6,
        .sum = 25.5,
        .min = 0.5,
        .max = 9.2,
        .boundaries = &boundaries,
        .bucket_counts = &bucket_counts,
    };

    const data_point = otel_sdk.metrics.MetricDataPoint{
        .attributes = &[_]otel_api.common.AttributeKeyValue{},
        .timestamp_ns = 1234567890,
        .start_timestamp_ns = 1234567000,
        .value = .{ .f64_histogram = histogram },
    };

    const metric_data = otel_sdk.metrics.MetricData{
        .resource = resource,
        .scope = scope,
        .name = "test_histogram",
        .description = "A test histogram",
        .unit = "ms",
        .type = .histogram,
        .data_points = &[_]otel_sdk.metrics.MetricDataPoint{data_point},
    };

    const metrics = [_]otel_sdk.metrics.MetricData{metric_data};
    var result = try convertToOtlpFormat(allocator, &metrics);
    defer result.deinit(allocator);

    try testing.expect(result.resource_metrics.items.len == 1);
    const rm = result.resource_metrics.items[0];
    try testing.expect(rm.scope_metrics.items.len == 1);

    const sm = rm.scope_metrics.items[0];
    try testing.expect(sm.metrics.items.len == 1);

    const metric = sm.metrics.items[0];
    try testing.expect(std.mem.eql(u8, metric.name, "test_histogram"));

    if (metric.data) |data| {
        switch (data) {
            .histogram => |hist| {
                try testing.expect(hist.data_points.items.len == 1);
                const hdp = hist.data_points.items[0];
                try testing.expectEqual(@as(u64, 6), hdp.count);
                if (hdp.sum) |sum| {
                    try testing.expectApproxEqAbs(@as(f64, 25.5), sum, 0.001);
                } else {
                    try testing.expect(false); // sum should be present
                }
            },
            else => try testing.expect(false),
        }
    } else {
        try testing.expect(false);
    }
}

test "OTLP metric protobuf bytes are stable" {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const resource_attrs = [_]otel_api.common.AttributeKeyValue{
        .{ .key = "service.name", .value = .{ .string = "golden" } },
    };
    const resource = otel_sdk.resource.Resource{ .attributes = &resource_attrs };
    const scope = otel_api.InstrumentationScope{ .name = "golden-scope", .version = "1.0.0" };
    const point_attrs = [_]otel_api.common.AttributeKeyValue{
        .{ .key = "route", .value = .{ .string = "/subscribe" } },
    };
    const boundaries = [_]f64{ 1.0, 5.0, 10.0 };
    const bucket_counts = [_]u64{ 2, 3, 1, 0 };
    const metrics = [_]otel_sdk.metrics.MetricData{
        .{
            .resource = resource,
            .scope = scope,
            .name = "requests_total",
            .description = "requests served",
            .unit = "1",
            .type = .sum,
            .data_points = &[_]otel_sdk.metrics.MetricDataPoint{.{
                .attributes = &point_attrs,
                .timestamp_ns = 1_700_000_000_000_000_000,
                .start_timestamp_ns = 1_699_999_000_000_000_000,
                .value = .{ .i64_sum = 42 },
            }},
        },
        .{
            .resource = resource,
            .scope = scope,
            .name = "queue_depth",
            .description = "pending rows",
            .unit = "1",
            .type = .gauge,
            .data_points = &[_]otel_sdk.metrics.MetricDataPoint{.{
                .attributes = &point_attrs,
                .timestamp_ns = 1_700_000_000_000_000_000,
                .start_timestamp_ns = null,
                .value = .{ .f64_gauge = 3.5 },
            }},
        },
        .{
            .resource = resource,
            .scope = scope,
            .name = "latency",
            .description = "request latency",
            .unit = "ms",
            .type = .histogram,
            .data_points = &[_]otel_sdk.metrics.MetricDataPoint{.{
                .attributes = &point_attrs,
                .timestamp_ns = 1_700_000_000_000_000_000,
                .start_timestamp_ns = 1_699_999_000_000_000_000,
                .value = .{ .f64_histogram = .{
                    .count = 6,
                    .sum = 25.5,
                    .min = 0.5,
                    .max = 9.2,
                    .boundaries = &boundaries,
                    .bucket_counts = &bucket_counts,
                } },
            }},
        },
    };
    var metrics_data = try convertToOtlpFormat(allocator, &metrics);
    var buffer = std.Io.Writer.Allocating.init(allocator);
    try metrics_data.encode(&buffer.writer, allocator);

    const hex = try std.fmt.allocPrint(allocator, "{x}", .{buffer.written()});
    try testing.expectEqualStrings(golden_metric_hex, hex);
}

// Recorded from zig 0.16 with zig-protobuf 4.0.0; pins the OTLP wire bytes
// across compiler and protobuf upgrades.
const golden_metric_hex =
    "0a93030a1a0a180a0c736572766963652e6e616d6512080a06676f6c64656e12f4020a150a0c676f6c64656e2d73636f" ++
    "70651205312e302e30125c0a0e72657175657374735f746f74616c120f7265717565737473207365727665641a01313a" ++
    "360a323a150a05726f757465120c0a0a2f7375627363726962651100f08461159c97171900002a36fe9c9717312a0000" ++
    "00000000001001124b0a0b71756575655f6465707468120c70656e64696e6720726f77731a01312a2b0a293a150a0572" ++
    "6f757465120c0a0a2f7375627363726962651900002a36fe9c9717210000000000000c4012af010a076c6174656e6379" ++
    "120f72657175657374206c6174656e63791a026d734a8e010a89014a150a05726f757465120c0a0a2f73756273637269" ++
    "62651100f08461159c97171900002a36fe9c971721060000000000000029000000000080394032200200000000000000" ++
    "0300000000000000010000000000000000000000000000003a18000000000000f03f0000000000001440000000000000" ++
    "244059000000000000e03f6166666666666622401001";

const TestServer = @import("test_server.zig").TestServer;

/// Two points of one counter (as a reader hands them over: a MetricData each) and a
/// cumulative sum, on a resource named "svc".
const fork_resource_attrs = [_]otel_api.common.AttributeKeyValue{.{ .key = "service.name", .value = .{ .string = "svc" } }};
const fork_get = [_]otel_api.common.AttributeKeyValue{.{ .key = "route", .value = .{ .string = "/a" } }};
const fork_post = [_]otel_api.common.AttributeKeyValue{.{ .key = "route", .value = .{ .string = "/b" } }};
fn forkMetrics() [3]MetricData {
    const resource = otel_sdk.resource.Resource{ .attributes = &fork_resource_attrs };
    const scope = otel_api.InstrumentationScope{ .name = "scope" };
    const point = struct {
        fn of(attrs: []const otel_api.common.AttributeKeyValue, v: i64) [1]MetricDataPoint {
            return .{.{ .attributes = attrs, .timestamp_ns = 2000, .start_timestamp_ns = 1000, .value = .{ .i64_sum = v } }};
        }
    }.of;
    const S = struct {
        const a = point(&fork_get, 3);
        const b = point(&fork_post, 4);
        const c = point(&fork_get, 9);
    };
    return .{
        .{ .resource = resource, .scope = scope, .name = "requests", .description = null, .unit = "1", .type = .sum, .data_points = &S.a, .is_monotonic = true },
        .{ .resource = resource, .scope = scope, .name = "requests", .description = null, .unit = "1", .type = .sum, .data_points = &S.b, .is_monotonic = true },
        .{ .resource = resource, .scope = scope, .name = "level", .description = null, .unit = "1", .type = .sum, .data_points = &S.c, .temporality = .cumulative },
    };
}

test "fork: an export goes to the endpoint's own port and path, with its resource, temporality and one Metric per instrument" {
    const testing = std.testing;
    var server: TestServer = undefined;
    try server.start(testing.io);
    defer server.stop();
    var url_buf: [64]u8 = undefined;
    var exporter = OtlpMetricExporter.init(testing.allocator, .{ .endpoint = server.url(&url_buf), .io = testing.io, .timeout_millis = 2000, .export_timeout_millis = 2000, .retry_config = .{ .enabled = false } });
    const metrics = forkMetrics();
    try testing.expectEqual(ExportResult.success, exporter.exportMetrics(&metrics));
    const st = exporter.stats();
    try testing.expectEqual(@as(u64, 1), st.exports_ok);
    try testing.expectEqual(@as(u64, 0), st.exports_failed);
    try testing.expect(st.last_export_bytes > 0 and st.last_export_ns > 0);

    const got = try server.last(testing.allocator);
    defer testing.allocator.free(got.path);
    defer testing.allocator.free(got.body);
    try testing.expectEqualStrings("/v1/metrics", got.path);
    try testing.expectEqual(@as(usize, st.last_export_bytes), got.body.len);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var reader: std.Io.Reader = .fixed(got.body);
    const decoded = try metrics_v1.MetricsData.decode(&reader, arena.allocator());
    const rm = decoded.resource_metrics.items[0];
    try testing.expectEqualStrings("service.name", rm.resource.?.attributes.items[0].key);
    try testing.expectEqualStrings("svc", rm.resource.?.attributes.items[0].value.?.value.?.string_value);
    const exported = rm.scope_metrics.items[0].metrics.items;
    try testing.expectEqual(@as(usize, 2), exported.len);
    for (exported) |m| {
        const sum = m.data.?.sum;
        if (std.mem.eql(u8, m.name, "requests")) {
            try testing.expectEqual(@as(usize, 2), sum.data_points.items.len);
            try testing.expect(sum.is_monotonic);
            try testing.expectEqual(metrics_v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, sum.aggregation_temporality);
        } else {
            try testing.expectEqualStrings("level", m.name);
            try testing.expect(!sum.is_monotonic);
            try testing.expectEqual(metrics_v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, sum.aggregation_temporality);
        }
    }

    // after shutdown, nothing is sent
    _ = exporter.shutdown(null);
    try testing.expectEqual(ExportResult.failure, exporter.exportMetrics(&metrics));
    try testing.expectEqual(@as(usize, 1), server.requests.load(.monotonic));
}

var fork_reported: std.atomic.Value(u32) = .init(0);
fn forkCountErrors(_: error_handler.ErrorInfo, _: ?std.mem.Allocator) void {
    _ = fork_reported.fetchAdd(1, .monotonic);
}

test "fork: a collector that never answers costs an export its timeout, and an outage is reported once" {
    const testing = std.testing;
    var server: TestServer = undefined;
    try server.start(testing.io);
    defer server.stop();
    server.hang.store(true, .release);
    var url_buf: [64]u8 = undefined;
    var exporter = OtlpMetricExporter.init(testing.allocator, .{ .endpoint = server.url(&url_buf), .io = testing.io, .timeout_millis = 200, .export_timeout_millis = 200, .retry_config = .{ .enabled = false } });
    const metrics = forkMetrics();

    fork_reported.store(0, .monotonic);
    error_handler.setGlobalErrorHandler(forkCountErrors);
    defer error_handler.setGlobalErrorHandler(null);
    var t0 = std.Io.Timestamp.now(testing.io, .awake).nanoseconds;
    try testing.expectEqual(ExportResult.failure, exporter.exportMetrics(&metrics));
    var took_ms = @divFloor(std.Io.Timestamp.now(testing.io, .awake).nanoseconds - t0, std.time.ns_per_ms);
    try testing.expect(took_ms >= 150 and took_ms < 1500);

    // a reader's shutdown budget cuts the timeout further
    exporter.config.timeout_millis = 5000;
    exporter.config.export_timeout_millis = 5000;
    t0 = std.Io.Timestamp.now(testing.io, .awake).nanoseconds;
    try testing.expectEqual(ExportResult.failure, exporter.exportMetricsWithin(&metrics, 150));
    took_ms = @divFloor(std.Io.Timestamp.now(testing.io, .awake).nanoseconds - t0, std.time.ns_per_ms);
    try testing.expect(took_ms < 1500);

    try testing.expectEqual(@as(u64, 2), exporter.stats().exports_failed);
    try testing.expectEqual(@as(u32, 1), fork_reported.load(.monotonic));
}
