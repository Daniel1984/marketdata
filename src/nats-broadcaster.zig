const std = @import("std");
const types = @import("./types.zig");

pub const NatsBroadcaster = struct {
    allocator: std.mem.Allocator,
    connection: ?std.net.Stream,
    host: []const u8,
    port: u16,
    mutex: std.Thread.Mutex,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, host: []const u8, port: u16) !Self {
        var self = Self{
            .allocator = allocator,
            .connection = null,
            .host = host,
            .port = port,
            .mutex = std.Thread.Mutex{},
        };

        try self.connect();
        return self;
    }

    pub fn deinit(self: *Self) void {
        self.disconnect();
    }

    fn connect(self: *Self) !void {
        self.connection = try std.net.tcpConnectToHost(self.allocator, self.host, self.port);
        std.log.info("Connected to NATS at {s}:{d}", .{ self.host, self.port });
    }

    fn disconnect(self: *Self) void {
        if (self.connection) |*conn| {
            conn.close();
            self.connection = null;
        }
    }

    fn reconnect(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check if another thread already reconnected
        if (self.connection != null) {
            return;
        }

        self.disconnect();
        std.log.info("Attempting to reconnect to NATS...", .{});

        // Try to reconnect with exponential backoff
        var attempts: u32 = 0;
        while (attempts < 5) {
            const backoff_ms = (@as(u64, attempts) + 1) * 1000; // 1s, 2s, 3s, 4s, 5s
            std.Thread.sleep(backoff_ms * std.time.ns_per_ms);

            self.connect() catch |err| {
                attempts += 1;
                std.log.warn("NATS reconnection attempt {} failed: {}", .{ attempts, err });
                continue;
            };

            std.log.info("Successfully reconnected to NATS", .{});
            return;
        }

        std.log.err("Failed to reconnect to NATS after {} attempts", .{attempts});
    }

    pub fn publishMessage(self: *Self, subject: []const u8, pld: types.MessageEnvelope) !void {
        const pld_json = try std.fmt.allocPrint(self.allocator, "{f}", .{std.json.fmt(pld, .{})});
        defer self.allocator.free(pld_json);

        const nats_msg = try std.fmt.allocPrint(self.allocator, "PUB {s} {d}\r\n{s}\r\n", .{ subject, pld_json.len, pld_json });
        defer self.allocator.free(nats_msg);

        if (self.connection) |conn| {
            conn.writeAll(nats_msg) catch |err| {
                std.log.err("write err: {}", .{err});
                self.reconnect();
                if (self.connection) |retry_conn| {
                    retry_conn.writeAll(nats_msg) catch |retry_err| {
                        std.log.err("reconnected write err 1: {}", .{retry_err});
                        return retry_err;
                    };
                } else {
                    return error.ConnectionFailed;
                }
            };
        } else {
            self.reconnect();
            if (self.connection) |conn| {
                conn.writeAll(nats_msg) catch |err| {
                    std.log.err("reconnected write err 2: {}", .{err});
                    return err;
                };
            } else {
                return error.ConnectionFailed;
            }
        }
    }
};
