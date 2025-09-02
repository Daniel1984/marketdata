const std = @import("std");
const kucoin = @import("./kucoin.zig");
const nats_broadcaster = @import("./nats-broadcaster.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var nats = try nats_broadcaster.NatsBroadcaster.init(allocator, "0.0.0.0", 4222);
    defer nats.deinit();

    var kc = kucoin.init(allocator, &nats);
    defer kc.deinit();

    try kc.getSocketConnectionDetails();
    try kc.connectWebSocket();
    try kc.subscribeChannel("/spotMarket/level2Depth5:BTC-USDT");
    try kc.consume();

    std.log.info("WebSocket connection closed", .{});
}
