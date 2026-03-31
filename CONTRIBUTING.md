# Contributing to bevault_workers

Thank you for your interest in contributing to **bevault_workers**, the library simplifies the creation of custom workers for beVault with pluggable data stores (PostgreSQL, S3, SFTP, and custom stores).

Contributions of all kinds are welcome: bug reports, feature requests, documentation improvements, and pull requests.

---

## Development setup

1. **Clone the repository**

   ```bash
   git clone <your-fork-or-remote-url>
   cd bevault_workers
   ```

2. **Python version**

   This project targets **Python 3.13+** (`requires-python` in `pyproject.toml`). Use a matching interpreter.

3. **Virtual environment** (recommended)

   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # Linux / macOS
   source .venv/bin/activate
   ```

4. **Install the package for development**

   Editable install with dev tools (pytest, black, flake8, mypy):

   ```bash
   pip install -e ".[dev]"
   ```

   Alternatively, you can install runtime deps from `requirements.txt` and then the package in editable mode:

   ```bash
   pip install -r requirements.txt
   pip install -e .
   ```

5. **Run the test suite**

   From the repository root:

   ```bash
   pytest
   ```

   Test paths and `pythonpath` are configured in `pyproject.toml` (`[tool.pytest.ini_options]`).

---

## Pre-commit

This repository includes a [pre-commit](https://pre-commit.com/) configuration in [`.pre-commit-config.yaml`](.pre-commit-config.yaml). Hooks may be organization-specific; install and run them locally before pushing.

### Install pre-commit

```bash
pip install pre-commit
```

### Enable hooks in this repository

```bash
pre-commit install
```

### Run hooks manually (all files)

```bash
pre-commit run --all-files
```

---

## Code style and checks

Optional dev dependencies in `pyproject.toml` include **black**, **flake8**, and **mypy**. Use them in line with team practice; if pre-commit runs formatting or lint steps, keep commits consistent with that output.

Examples:

```bash
black src tests
flake8 src tests
mypy src
```

Adjust paths or configuration as your local `mypy` / `flake8` setup requires.

---

## Project layout (this repository)

- **`src/bevault_workers/`** — library code (workers, stores, AWS States store sync helpers, utilities).
- **`tests/`** — pytest tests; shared fixtures live in `tests/conftest.py`.
- **`pyproject.toml`** — package metadata, dependencies, pytest options.
- **`README.md`** — user-facing documentation and examples.

When adding stores or workers, follow existing patterns under `src/bevault_workers/stores/` and `src/bevault_workers/workers/`.

---

## Submitting changes

1. **Branch** from the appropriate base (for example `develop` or `main`, depending on your workflow).

   ```bash
   git checkout -b feature/my-change
   ```

2. **Implement** focused, reviewable changes; extend **tests** when behavior changes.

3. **Run** `pytest` and pre-commit (if you use it) before opening a PR.

4. **Push** and open a **Pull Request** with:

   - A clear description of the problem or feature.
   - Links to issues or tickets, if any.
   - Notes on breaking changes or migration steps when relevant.

### Commit messages

We recommend **[Conventional Commits](https://www.conventionalcommits.org/)**:

```text
type(scope): short description
```

Common types:

| Type | Use |
|------|-----|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `refactor` | Internal refactor without behavior change |
| `test` | Tests only |
| `chore` | Tooling, CI, maintenance |

Examples:

- `feat(stores): support States-configured S3 endpoints`
- `fix(worker): handle shutdown race in process pool`
- `docs: clarify config.json store examples`

---

## Licensing

By contributing, you agree that your contributions are licensed under the same terms as this repository. See [`LICENSE`](LICENSE) (MIT).

---

## Questions

If something is unclear, open an issue or ask in your PR. Suggestions to improve this document are welcome via issue or PR.
