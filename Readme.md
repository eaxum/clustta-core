<a href="https://clustta.com">
  <img src="./assets/clustta-logo.svg" alt="Clustta" style="width: 60px; height: 60px;" />
</a>


# Clustta Core - shared Go packages for the Clustta ecosystem

Clustta Core contains the shared Go packages used across the Clustta ecosystem. These packages are imported by the client, server, and studio repositories to ensure consistent behavior and reduce code duplication.

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

## Packages

| Package | Description |
|---------|-------------|
| `constants` | Shared configuration variables (host URLs, user agent) with build-time override support via ldflags |
| `errors` | Canonical error variables for all domain types (assets, collections, workflows, etc.) |
| `ignore` | Gitignore-style pattern matching for file filtering |
| `system_icon` | Platform-specific file extension icon extraction (Windows + macOS) |

## Installation

```bash
go get github.com/eaxum/clustta-core
```

## Usage

```go
import (
    "github.com/eaxum/clustta-core/constants"
    "github.com/eaxum/clustta-core/errors"
    "github.com/eaxum/clustta-core/ignore"
    "github.com/eaxum/clustta-core/system_icon"
)
```

### Local Development

When developing across repos locally, use a `replace` directive in your `go.mod`:

```go
replace github.com/eaxum/clustta-core => ../clustta-core
```

Remove it before committing.

## Ecosystem

Clustta Core is consumed by:

1. **[Clustta Client](https://github.com/eaxum/clustta-client)** - The desktop application (Wails v3 + Vue 3)
2. **[Clustta Server](https://github.com/eaxum/clustta-server)** - The global authentication and project hosting server
3. **[Clustta Studio](https://github.com/eaxum/clustta-studio)** - The studio/team management server

## License

Clustta Core is released under the GNU Affero General Public License v3.0. See the [LICENSE](LICENSE) file for details.

## About

Clustta is developed by [Eaxum](https://eaxum.com), a computer animation studio based in Nigeria.

<a href="https://eaxum.com">
  <img src="./assets/eaxum-logo.gif" alt="Eaxum" style="height: 100px" />
</a>
