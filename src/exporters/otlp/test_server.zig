//! A collector stand-in for the exporters' tests: one loopback listener on a thread that
//! answers each POST with 200, or never answers. It keeps the last request's path and body.
const std = @import("std");
const Io = std.Io;

pub const TestServer = struct {
    io: Io,
    server: Io.net.Server,
    port: u16,
    thread: std.Thread,
    stopping: std.atomic.Value(bool) = .init(false),
    /// Never answer: a request stays out until `stop`.
    hang: std.atomic.Value(bool) = .init(false),
    requests: std.atomic.Value(usize) = .init(0),
    mutex: Io.Mutex = .init,
    path: std.ArrayList(u8) = .empty,
    body: std.ArrayList(u8) = .empty,

    const gpa = std.heap.page_allocator;

    pub fn start(self: *TestServer, io: Io) !void {
        const any = try Io.net.IpAddress.parse("127.0.0.1", 0);
        self.* = .{ .io = io, .server = try any.listen(io, .{}), .port = undefined, .thread = undefined };
        self.port = self.server.socket.address.getPort();
        self.thread = try std.Thread.spawn(.{}, serve, .{self});
    }

    /// "http://127.0.0.1:<port>"
    pub fn url(self: *const TestServer, buf: []u8) []const u8 {
        return std.fmt.bufPrint(buf, "http://127.0.0.1:{d}", .{self.port}) catch unreachable;
    }

    pub fn stop(self: *TestServer) void {
        self.stopping.store(true, .release);
        // wake the accept
        const addr = Io.net.IpAddress.parse("127.0.0.1", self.port) catch unreachable;
        if (addr.connect(self.io, .{ .mode = .stream })) |s| s.close(self.io) else |_| {}
        self.thread.join();
        self.server.deinit(self.io);
        self.path.deinit(gpa);
        self.body.deinit(gpa);
    }

    /// The last request's path and body, copied.
    pub fn last(self: *TestServer, allocator: std.mem.Allocator) !struct { path: []u8, body: []u8 } {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const p = try allocator.dupe(u8, self.path.items);
        errdefer allocator.free(p);
        return .{ .path = p, .body = try allocator.dupe(u8, self.body.items) };
    }

    fn serve(self: *TestServer) void {
        while (!self.stopping.load(.acquire)) {
            const stream = self.server.accept(self.io) catch continue;
            defer stream.close(self.io);
            self.connection(stream) catch {};
        }
    }

    fn connection(self: *TestServer, stream: Io.net.Stream) !void {
        var rbuf: [16 * 1024]u8 = undefined;
        var wbuf: [256]u8 = undefined;
        var r = stream.reader(self.io, &rbuf);
        var w = stream.writer(self.io, &wbuf);
        while (!self.stopping.load(.acquire)) {
            var content_length: usize = 0;
            var first = true;
            while (true) {
                const line = r.interface.takeDelimiterInclusive('\n') catch |e| switch (e) {
                    error.EndOfStream => return,
                    else => return e,
                };
                const trimmed = std.mem.trimEnd(u8, line, "\r\n");
                if (first) {
                    first = false;
                    var parts = std.mem.tokenizeScalar(u8, trimmed, ' ');
                    _ = parts.next();
                    self.mutex.lockUncancelable(self.io);
                    defer self.mutex.unlock(self.io);
                    self.path.clearRetainingCapacity();
                    try self.path.appendSlice(gpa, parts.next() orelse "");
                    continue;
                }
                if (trimmed.len == 0) break;
                const colon = std.mem.indexOfScalar(u8, trimmed, ':') orelse continue;
                if (std.ascii.eqlIgnoreCase(trimmed[0..colon], "content-length"))
                    content_length = try std.fmt.parseInt(usize, std.mem.trim(u8, trimmed[colon + 1 ..], " "), 10);
            }
            {
                self.mutex.lockUncancelable(self.io);
                defer self.mutex.unlock(self.io);
                try self.body.resize(gpa, content_length);
                try r.interface.readSliceAll(self.body.items);
            }
            _ = self.requests.fetchAdd(1, .monotonic);
            if (self.hang.load(.acquire)) {
                while (!self.stopping.load(.acquire)) self.io.sleep(.fromMilliseconds(5), .awake) catch {};
                return;
            }
            try w.interface.writeAll("HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\n\r\n");
            try w.interface.flush();
        }
    }
};
