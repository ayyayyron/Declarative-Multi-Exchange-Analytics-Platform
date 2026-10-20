# MEXC Protobuf provenance

The unmodified reference files under `upstream/mexc/` come from:

- Repository: <https://github.com/mexcdevelop/websocket-proto>
- Commit: `7b8ac7a6681f28551612a5a7cefbb7e09b56bb85`
- Retrieved: 2026-07-28
- License: Apache-2.0; see `LICENSE.mexc`

`mexc_market_data.proto` is the local decoder schema. It retains only the
aggregate-deal fields used by this project, with the same wire field numbers as
the pinned upstream definitions. Protobuf safely ignores the wrapper's unrelated
public and private channel fields, so the application does not generate or ship
unused exchange account models.
