const std = @import("std");
const ws = @import("websocket");
const request = @import("./request.zig");
const json = std.json;
const crypto = std.crypto;

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

const MessageEnvelope = struct {
    type: []const u8,
    source: []const u8,
    data: []const u8,
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
last_nats_error_time: i64,
nats_error_count: u32,

pub fn init(allocator: std.mem.Allocator) !Self {
    var self = Self{
        .allocator = allocator,
        .token = null,
        .endpoint = null,
        .ping_interval = 18000,
        .ping_timeout = 10000,
        .mutex = std.Thread.Mutex{},
        .client = null,
        .nats = null,
        .last_nats_error_time = 0,
        .nats_error_count = 0,
    };

    try self.connectNATS();
    return self;
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
    if (self.nats) |*nats| {
        nats.close();
        self.nats = null;
    }
}

fn connectNATS(self: *Self) !void {
    // Connect to NATS (assuming localhost:4222), TODO: make env var driven
    self.nats = try std.net.tcpConnectToHost(self.allocator, "0.0.0.0", 4222);
    std.log.info("Connected to NATS", .{});
    self.nats_error_count = 0; // Reset error count on successful connection
}

fn reconnectNATS(self: *Self) void {
    if (self.nats) |*nats| {
        nats.close();
        self.nats = null;
    }

    std.log.info("Attempting to reconnect to NATS...", .{});

    // Try to reconnect with backoff
    var attempts: u32 = 0;
    while (attempts < 5) {
        std.Thread.sleep(std.time.ns_per_s * (@as(u64, attempts) + 1)); // 1s, 2s, 3s, 4s, 5s backoff

        self.connectNATS() catch |err| {
            attempts += 1;
            std.log.warn("NATS reconnection attempt {} failed: {}", .{ attempts, err });
            continue;
        };

        std.log.info("Successfully reconnected to NATS", .{});
        return;
    }

    std.log.err("Failed to reconnect to NATS after {} attempts", .{attempts});
}

fn handleNATSError(self: *Self, err: anyerror) void {
    const current_time = std.time.timestamp();

    // Only log errors once every 30 seconds to prevent spam
    if (current_time - self.last_nats_error_time > 30) {
        std.log.err("NATS error (count: {}): {}", .{ self.nats_error_count + 1, err });
        self.last_nats_error_time = current_time;
        self.nats_error_count = 0;
    } else {
        self.nats_error_count += 1;
    }
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
                                    // Create envelope
                                    const envelope = MessageEnvelope{
                                        .type = "orderbook",
                                        .source = "kucoin",
                                        .data = msg.data,
                                    };

                                    // Serialize envelope to JSON
                                    const envelope_json = try std.fmt.allocPrint(self.allocator, "{f}", .{std.json.fmt(envelope, .{})});
                                    defer self.allocator.free(envelope_json);

                                    const nats_msg = try std.fmt.allocPrint(self.allocator, "PUB {s} {d}\r\n{s}\r\n", .{ "market", envelope_json.len, envelope_json });
                                    defer self.allocator.free(nats_msg);

                                    if (self.nats) |nats| {
                                        nats.writeAll(nats_msg) catch |err| {
                                            self.handleNATSError(err);
                                            // Single retry after reconnection attempt
                                            self.reconnectNATS();
                                            if (self.nats) |retry_nats| {
                                                retry_nats.writeAll(nats_msg) catch |retry_err| {
                                                    self.handleNATSError(retry_err);
                                                };
                                            }
                                        };
                                    } else {
                                        self.handleNATSError(error.NullConnection);
                                        self.reconnectNATS();
                                        if (self.nats) |nats| {
                                            nats.writeAll(nats_msg) catch |err| {
                                                self.handleNATSError(err);
                                            };
                                        }
                                    }
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
