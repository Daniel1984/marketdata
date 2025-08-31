const std = @import("std");
const ws = @import("websocket");
const request = @import("./request.zig");
const json = std.json;
const crypto = std.crypto;

const TickerDataNumeric = struct {
    bestAsk: f64,
    bestAskSize: f64,
    bestBid: f64,
    bestBidSize: f64,
    price: f64,
    sequence: u64,
    size: f64,
    time: i64,
};

const MarketTickerNumeric = struct {
    topic: []const u8,
    type: []const u8,
    subject: []const u8,
    data: TickerDataNumeric,
};

const KuCoinTokenResponse = struct {
    code: []const u8,
    data: struct {
        token: []const u8,
        instanceServers: []struct {
            endpoint: []const u8,
            encrypt: bool,
            protocol: []const u8,
            pingInterval: u64,
            pingTimeout: u64,
        },
    },
};

const SubscribeMessage = struct {
    id: u64,
    type: []const u8,
    topic: []const u8,
    response: bool,
};

pub const Self = @This();

allocator: std.mem.Allocator,
token: ?[]u8,
endpoint: ?[]u8,
ping_interval: u64,
ping_timeout: u64,
mutex: std.Thread.Mutex,
client: ?ws.Client,
nats: ?std.net.Stream,

pub fn init(allocator: std.mem.Allocator) !Self {
    // Connect to NATS (assuming localhost:4222), TODO: make env var driven
    const natsStream = try std.net.tcpConnectToHost(allocator, "0.0.0.0", 4222);

    return Self{
        .allocator = allocator,
        .token = null,
        .endpoint = null,
        .ping_interval = 18000,
        .ping_timeout = 10000,
        .mutex = std.Thread.Mutex{},
        .client = null,
        .nats = natsStream,
    };
}

pub fn deinit(self: *Self) void {
    if (self.token) |token| self.allocator.free(token);
    if (self.endpoint) |endpoint| self.allocator.free(endpoint);
    if (self.client) |*client| client.deinit();
    if (self.nats) |*nats| nats.close();
}

pub fn getSocketConnectionDetails(self: *Self) !void {
    if (!self.mutex.tryLock()) return;
    defer self.mutex.unlock();

    const body = try request.post(self.allocator, "https://api.kucoin.com/api/v1/bullet-public");
    defer self.allocator.free(body);

    const parsedBody = try json.parseFromSlice(KuCoinTokenResponse, self.allocator, body, .{ .ignore_unknown_fields = true });
    defer parsedBody.deinit();

    if (!std.mem.eql(u8, parsedBody.value.code, "200000")) return error.ConnectionError;
    if (parsedBody.value.data.instanceServers.len == 0) return error.MissingInstanceServers;

    self.token = self.allocator.dupe(u8, parsedBody.value.data.token) catch return error.MissingToken;
    self.endpoint = self.allocator.dupe(u8, parsedBody.value.data.instanceServers[0].endpoint) catch return error.MissingEndpoint;
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

    self.client = try ws.Client.init(self.allocator, .{
        .port = port,
        .host = host,
        .tls = is_tls,
        .max_size = 65536,
        .buffer_size = 4096,
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

    const subscribe_msg = SubscribeMessage{
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
            try self.client.?.writePing(&ping_data);
            std.log.info("Sent ping", .{});
            ping_timer.reset();
        }

        const message = self.client.?.read() catch |err| {
            std.log.err("failed reading raw message: {}", .{err});
            continue;
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
                                    const nats_msg = try std.fmt.allocPrint(self.allocator, "PUB {s} {d}\r\n{s}\r\n", .{ "market", msg.data.len, msg.data });
                                    errdefer self.allocator.free(nats_msg);
                                    defer self.allocator.free(nats_msg);

                                    self.nats.?.writeAll(nats_msg) catch |err| {
                                        std.log.err("Failed to write message to NATS: {}", .{err});
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

fn parseNumericTicker(self: *Self, json_string: []const u8) !MarketTickerNumeric {
    const parsed = try std.json.parseFromSlice(MarketTickerNumeric, self.allocator, json_string, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const ticker = parsed.value;

    // Convert string values to numeric
    return MarketTickerNumeric{
        .topic = try self.allocator.dupe(u8, ticker.topic),
        .type = try self.allocator.dupe(u8, ticker.type),
        .subject = try self.allocator.dupe(u8, ticker.subject),
        .data = TickerDataNumeric{
            .bestAsk = try std.fmt.parseFloat(f64, ticker.data.bestAsk),
            .bestAskSize = try std.fmt.parseFloat(f64, ticker.data.bestAskSize),
            .bestBid = try std.fmt.parseFloat(f64, ticker.data.bestBid),
            .bestBidSize = try std.fmt.parseFloat(f64, ticker.data.bestBidSize),
            .price = try std.fmt.parseFloat(f64, ticker.data.price),
            .sequence = try std.fmt.parseInt(u64, ticker.data.sequence, 10),
            .size = try std.fmt.parseFloat(f64, ticker.data.size),
            .time = ticker.data.time,
        },
    };
}
