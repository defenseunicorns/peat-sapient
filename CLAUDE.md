# CLAUDE.md — peat-sapient

Before doing any work in this repo, read `SKILL.md`.

## Quick orientation

- **Repo role:** SAPIENT (BSI Flex 335 v2.0) protocol library and Peat bridge. Also hosts `peat-mesh-sapient`, the one-way `peat_mesh::transport::Translator`/`Transport` adapter (ADR-059 Amendment 4) — see `docs/PLAN.md` and `peat-mesh-sapient/src/lib.rs`.
- **Repo layout:** Cargo workspace, two members — `peat-sapient/` (the library; zero `peat-mesh` dependency, ever) and `peat-mesh-sapient/` (depends on both `peat-mesh` and `peat-sapient`, one-way).
- **Primary language:** Rust
- **Sanity check:** `cargo check -p peat-sapient --features peat,translator-codec && cargo check -p peat-mesh-sapient`

## Hard rule

A task in this repo is not done until the verification checklist in `SKILL.md` produces evidence.

## Hard rule: FIPS-approved cryptographic primitives only

See `peat/CLAUDE.md` §"Hard rule: FIPS-approved cryptographic primitives only" — applies here too.

## Hard rule: no consumer-specific references

See `peat/CLAUDE.md` §"Hard rule: no consumer-specific references in peat" — applies here too.
