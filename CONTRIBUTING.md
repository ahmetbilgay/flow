# Contributing to Dgmos Flow

First off, thank you for considering contributing to Dgmos Flow! It's people like you that make the open-source community such an amazing place to learn, inspire, and create.

## How Can I Contribute?

### Reporting Bugs
Before creating a bug report, please check that an issue doesn't already exist. When filing a bug, please include:
- A clear and descriptive title
- Steps to reproduce the behavior
- Expected vs Actual behavior
- Go version and OS

### Suggesting Enhancements
We love new ideas! If you have a suggestion for an enhancement, please open an issue with the "feature request" template.

### Pull Requests
1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. Ensure unit tests pass: `go test ./... -race`
4. Run integration tests when touching storage/triggers: `bash scripts/test-integration.sh`
5. Make sure your code is linted (`golangci-lint run ./...`).
6. Issue the Pull Request!

## Development Setup
1. Clone the repo.
2. Run `docker compose up -d` to start MySQL, PostgreSQL, and Redis.
3. Unit tests: `go test ./... -race`
4. Integration / E2E: `bash scripts/test-integration.sh` (or `go test -tags=integration ./...`)

---
Happy coding!
