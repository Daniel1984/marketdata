const std = @import("std");
const http = std.http;

pub fn post(allocator: std.mem.Allocator, url: []const u8) ![]u8 {
    var client: std.http.Client = .{
        .allocator = allocator,
    };
    defer client.deinit();

    var response: std.Io.Writer.Allocating = .init(allocator);

    const result = try client.fetch(.{
        .method = .POST,
        .payload = "",
        .location = .{ .url = url },
        .response_writer = &response.writer,
        .headers = .{ .accept_encoding = .{ .override = "identity" } },
    });

    if (result.status != .ok) {
        return error.HttpRequestFailed;
    }

    return try response.toOwnedSlice();
}
