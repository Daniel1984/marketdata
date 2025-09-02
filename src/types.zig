pub const KuCoinTokenResponse = struct {
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

pub const SubscribeMessage = struct {
    id: u64,
    type: []const u8,
    topic: []const u8,
    response: bool,
};

pub const MessageEnvelope = struct {
    type: []const u8,
    source: []const u8,
    data: []const u8,
};
