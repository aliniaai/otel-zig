# harbor's fork of otel-zig

- **Branch `harbor`** is what [aliniaai/harbor](https://github.com/aliniaai/harbor) builds,
  as the submodule `services/moderations/vendor/otel-zig`: upstream's `v0.1.0-alpha.2` plus
  harbor's fixes, never upstreamed. `git log v0.1.0-alpha.2..harbor` is the list. Rebased
  onto upstream's releases as they come, keeping what upstream fixed its way.
- **Origin:** <https://github.com/zzstoatzz/otel-zig>, itself a fork of Jason Watson's
  otel-zig. BSD 3-Clause: [LICENSE](LICENSE).
- **Tests:** `zig build test` here; harbor runs the same suite as `zig build test:otel` in
  `services/moderations`, part of its `zig build test`, so its CI runs them.
- `main` here still points at upstream's `v0.1.0-alpha.1`; other branches (`batch-export`)
  are earlier, separate work. `harbor` uses neither.
