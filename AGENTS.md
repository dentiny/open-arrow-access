# Architecture checks

- Run `make lint-deslop` after changing Rust code. See `docs/testing.md` for setup.
- Use [deslop-please](https://github.com/deterministic-machines/deslop-please) to
  check code structure. Fix new diagnostics from first principles; do not move
  code mechanically to hide them.
- For `module-cycle` and `tangle`, decide which abstraction layer owns each
  responsibility and which direction dependencies should flow.
- For `field-leak` and `shallow-module`, consider usage patterns together and hide
  implementation details behind a narrow interface.
- Show a before/after diagram when proposing an architectural restructuring.
- `.deslop-please.allow` records existing debt. Do not regenerate it just to make
  a check pass. Review intentional updates and remove entries as debt is fixed.
