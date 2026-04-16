# REAL/PAPER Mode Audit

This document checks where mode-aware behavior exists in the repository.

## Core mode switch

- `config.py`
  - `EXECUTION_MODE` is sourced from env and normalized via `.upper()`.
  - Default is `PAPER`.

## Order pipeline

- `order.py`
  - `_is_real_mode()` gates live behavior.
  - In `REAL`, engine syncs exchange positions/open orders and submits live entries.
  - In `PAPER`, entry lifecycle remains local/simulated.

## Position lifecycle

- `position.py`
  - Binance client creation is deferred and blocked unless mode is `REAL`.
  - `REAL`: relies on exchange fill/protection synchronization.
  - `PAPER`: uses synthetic protection identifiers (`paper-sl` / `paper-tp`) and local close flow.

## Shared components (mode-agnostic)

- `strategy.py` / `market.py` / `utils.py`
  - Provide setup, data, and helper logic used by both modes.

## Live exchange adapter

- `binance_real.py`
  - Centralized live Binance Futures API integration consumed only when mode requires it.

## Notes

- `EXECUTION_MODE` accepts `PAPER` or `REAL`.
- Use `PAPER` as the default validation path before enabling `REAL`.
