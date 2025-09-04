const std = @import("std");
const ws = @import("websocket");
const zimq = @import("zimq");
const request = @import("./request.zig");
const types = @import("./types.zig");
const str = @import("./stream.zig");
const json = std.json;
const crypto = std.crypto;

pub const Self = @This();

allocator: std.mem.Allocator,
token: ?[]u8,
endpoint: ?[]u8,
ping_interval: u64,
ping_timeout: u64,
mutex: std.Thread.Mutex,
client: ?ws.Client,
stream: str.Self,

pub fn init(allocator: std.mem.Allocator, stream: str.Self) !Self {
    return Self{
        .allocator = allocator,
        .token = null,
        .endpoint = null,
        .ping_interval = 18000,
        .ping_timeout = 10000,
        .mutex = std.Thread.Mutex{},
        .client = null,
        .stream = stream,
    };
}

pub fn deinit(self: *Self) void {
    if (self.token) |token| {
        self.allocator.free(token);
        self.token = null;
    }
    if (self.endpoint) |endpoint| {
        self.allocator.free(endpoint);
        self.endpoint = null;
    }
    if (self.client) |*client| {
        client.deinit();
        self.client = null;
    }
}

pub fn getSocketConnectionDetails(self: *Self) !void {
    if (!self.mutex.tryLock()) return;
    defer self.mutex.unlock();

    const body = try request.post(self.allocator, "https://api.kucoin.com/api/v1/bullet-public");
    defer self.allocator.free(body);

    const parsedBody = try json.parseFromSlice(types.KuCoinTokenResponse, self.allocator, body, .{ .ignore_unknown_fields = true });
    defer parsedBody.deinit();

    if (!std.mem.eql(u8, parsedBody.value.code, "200000")) return error.ConnectionError;
    if (parsedBody.value.data.instanceServers.len == 0) return error.MissingInstanceServers;

    // Free existing allocations before creating new ones
    if (self.token) |token| {
        self.allocator.free(token);
        self.token = null;
    }
    if (self.endpoint) |endpoint| {
        self.allocator.free(endpoint);
        self.endpoint = null;
    }

    // Use temporary variables to avoid partial state on error
    const token = try self.allocator.dupe(u8, parsedBody.value.data.token);
    errdefer self.allocator.free(token);

    const endpoint = try self.allocator.dupe(u8, parsedBody.value.data.instanceServers[0].endpoint);
    errdefer self.allocator.free(endpoint);

    // Only assign after both succeed
    self.token = token;
    self.endpoint = endpoint;
    self.ping_interval = parsedBody.value.data.instanceServers[0].pingInterval;
    self.ping_timeout = parsedBody.value.data.instanceServers[0].pingTimeout;

    std.log.info("token: {s}", .{self.token.?});
    std.log.info("endpoint: {s}", .{self.endpoint.?});
}

pub fn connectWebSocket(self: *Self) !void {
    const uri = try std.Uri.parse(self.endpoint.?);

    // Extract host safely
    const host_component = uri.host.?;
    const host = switch (host_component) {
        .raw => |raw| raw,
        .percent_encoded => |encoded| encoded,
    };

    const port: u16 = uri.port orelse if (std.mem.eql(u8, uri.scheme, "wss")) 443 else 80;
    const is_tls = std.mem.eql(u8, uri.scheme, "wss");

    // Clean up existing client if it exists
    if (self.client) |*client| {
        client.deinit();
        self.client = null;
    }

    self.client = try ws.Client.init(self.allocator, .{
        .port = port,
        .host = host,
        .tls = is_tls,
        .max_size = 4096,
        .buffer_size = 1024,
    });

    const request_path = try std.fmt.allocPrint(self.allocator, "/?token={s}", .{self.token.?});
    defer self.allocator.free(request_path);

    const headers = try std.fmt.allocPrint(self.allocator, "Host: {s}", .{host});
    defer self.allocator.free(headers);

    try self.client.?.handshake(request_path, .{
        .timeout_ms = 10000,
        .headers = headers,
    });

    std.log.info("socket connection established!", .{});
}

pub fn subscribeChannel(self: *Self, topic: []const u8) !void {
    // Generate random ID
    var random_bytes: [8]u8 = undefined;
    crypto.random.bytes(&random_bytes);
    var id: u64 = 0;
    for (random_bytes) |byte| {
        id = (id << 8) | byte;
    }

    const subscribe_msg = types.SubscribeMessage{
        .id = id,
        .type = "subscribe",
        .topic = topic,
        .response = true,
    };

    const subscribe_json = try std.fmt.allocPrint(self.allocator, "{f}", .{std.json.fmt(subscribe_msg, .{})});
    defer self.allocator.free(subscribe_json);

    std.log.info("subscribing to {s}...", .{topic});
    std.log.info("subscription payload: {s}", .{subscribe_json});

    self.client.?.write(subscribe_json) catch |err| {
        std.log.err("Failed to subscribe to {s}: {}", .{ topic, err });
    };
}

pub fn consume(self: *Self) !void {
    // Set a read timeout
    try self.client.?.readTimeout(5000); // 5 second timeout

    // Handle incoming messages
    var ping_timer = try std.time.Timer.start();
    const ping_interval_ns = self.ping_interval * std.time.ns_per_ms;

    while (true) {
        // Check if we need to send a ping
        if (ping_timer.read() >= ping_interval_ns) {
            var ping_data = [_]u8{};
            self.client.?.writePing(&ping_data) catch |err| {
                std.log.err("Failed to send ping: {}", .{err});
                return err; // Exit consume loop on ping failure
            };
            std.log.info("Sent ping", .{});
            ping_timer.reset();
        }

        const message = self.client.?.read() catch |err| {
            std.log.err("failed reading raw message: {}", .{err});
            return err; // Exit consume loop on read failure
        };

        if (message) |msg| {
            defer self.client.?.done(msg);

            switch (msg.type) {
                .text => {
                    // std.log.info("Received: {s}", .{msg.data});

                    const parsed = std.json.parseFromSlice(std.json.Value, self.allocator, msg.data, .{}) catch |err| {
                        std.log.warn("Failed to parse message as JSON: {}", .{err});
                        continue;
                    };
                    defer parsed.deinit();

                    if (parsed.value.object.get("type")) |msg_type| {
                        switch (msg_type) {
                            .string => |type_str| {
                                if (std.mem.eql(u8, type_str, "welcome")) {
                                    std.log.info("✓ Welcome message received", .{});
                                    continue;
                                }

                                if (std.mem.eql(u8, type_str, "ack")) {
                                    std.log.info("✓ Subscription acknowledged", .{});
                                    continue;
                                }

                                if (std.mem.eql(u8, type_str, "message")) {
                                    // std.debug.print("pushing to zmq...", .{});
                                    self.stream.publishMessage(.{ .data = msg.data, .type = "orderbook", .source = "kucoin" }) catch |err| {
                                        std.log.warn("failed publishing msg: {}", .{err});
                                    };
                                    continue;
                                }

                                if (std.mem.eql(u8, type_str, "error")) {
                                    std.log.err("❌ WebSocket error: {s}", .{msg.data});
                                }
                            },
                            else => {},
                        }
                    }
                },
                .binary => {
                    std.log.info("Received binary message of {} bytes", .{msg.data.len});
                },
                .ping => {
                    std.log.info("Received ping", .{});
                    const pong_data = try self.allocator.dupe(u8, msg.data);
                    defer self.allocator.free(pong_data);
                    try self.client.?.writePong(pong_data);
                },
                .pong => {
                    std.log.info("Received pong", .{});
                },
                .close => {
                    std.log.info("WebSocket connection closed by server", .{});
                    try self.client.?.close(.{});
                    break;
                },
            }
        }
    }

    std.log.info("WebSocket connection closed", .{});
}
