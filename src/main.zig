const std = @import("std");
const kucoin = @import("./kucoin.zig");
const stream = @import("./stream.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var publisher = try stream.init(allocator, .{});
    try publisher.connect();

    var kc = try kucoin.init(allocator, publisher);
    defer kc.deinit();

    try kc.getSocketConnectionDetails();
    try kc.connectWebSocket();
    try kc.subscribeChannel("/spotMarket/level2Depth5:BTC-USDT");
    try kc.consume();

    std.log.info("WebSocket connection closed", .{});
}
