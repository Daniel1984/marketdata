const std = @import("std");
const zimq = @import("zimq");
const types = @import("./types.zig");

pub const Self = @This();

allocator: std.mem.Allocator,
stream_url: [:0]const u8,
mutex: std.Thread.Mutex,
context: ?*zimq.Context,
socket: ?*zimq.Socket,

pub const Opts = struct {
    stream_url: []const u8 = "tcp://localhost:5555",
};

pub fn init(allocator: std.mem.Allocator, opts: Opts) !Self {
    return Self{
        .allocator = allocator,
        .stream_url = try allocator.dupeZ(u8, opts.stream_url),
        .mutex = std.Thread.Mutex{},
        .context = null,
        .socket = null,
    };
}

pub fn deinit(self: *Self) void {
    self.allocator.free(self.stream_url);
    self.disconnect();
}

fn disconnect(self: *Self) void {
    if (self.context) |ctx| {
        ctx.deinit();
        self.context = null;
    }
    if (self.socket) |sock| {
        sock.deinit();
        self.socket = null;
    }
}

pub fn connect(self: *Self) !void {
    self.context = try zimq.Context.init();
    self.socket = try zimq.Socket.init(self.context.?, .push);
    try self.socket.?.connect(self.stream_url);
    std.debug.print("data stream connected!\n", .{});
}

fn reconnect(self: *Self) void {
    self.mutex.lock();
    defer self.mutex.unlock();

    // check if another thread already reconnected
    if (self.socket != null) {
        return;
    }

    self.disconnect();
    std.log.info("attempting to reconnect stream...", .{});

    // Try to reconnect with exponential backoff
    var attempts: u32 = 0;
    while (attempts < 5) {
        const backoff_ms = (@as(u64, attempts) + 1) * 1000; // 1s, 2s, 3s, 4s, 5s
        std.Thread.sleep(backoff_ms * std.time.ns_per_ms);

        self.connect() catch |err| {
            attempts += 1;
            std.log.warn("stream reconnection attempt {} failed: {}", .{ attempts, err });
            continue;
        };

        std.log.info("stream reconnected!", .{});
        return;
    }

    std.log.err("failed to reconnect stream after {} attempts", .{attempts});
}

pub fn publishMessage(self: *Self, pld: types.MessageEnvelope) !void {
    const pld_json = try std.fmt.allocPrint(self.allocator, "{f}", .{std.json.fmt(pld, .{})});
    defer self.allocator.free(pld_json);

    if (self.socket) |socket| {
        socket.sendSlice(pld_json, .{}) catch |err| {
            std.log.err("write to stream err: {}", .{err});
            self.reconnect();
            if (self.socket) |retry_socket| {
                retry_socket.sendSlice(pld_json, .{}) catch |retry_err| {
                    std.log.err("reconnected failet to publish msg: {}", .{retry_err});
                    return retry_err;
                };
            } else {
                return error.ConnectionFailed;
            }
        };
    } else {
        self.reconnect();
        if (self.socket) |socket| {
            socket.sendSlice(pld_json, .{}) catch |err| {
                std.log.warn("failed to publish msg: {}", .{err});
                return err;
            };
        } else {
            return error.ConnectionFailed;
        }
    }
}
