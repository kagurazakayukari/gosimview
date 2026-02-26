# GoSimView

A complete reverse-engineered rewrite of SimView in Go, providing data processing, analysis, and visualization for Assetto Corsa racing simulator.

### Motivation
Created initially out of frustration with naming conflicts between User and Team in team events, this project evolved into a complete rewrite due to sudden interest in the codebase and because the original author became unresponsive. 

## Quick Start

### Prerequisites
- Go 1.25+
- MySQL database
- Assetto Corsa game server

### Installation
1. Clone this repository
2. Install dependencies: `go mod download`
3. Obtain required external files (see below)
4. Configure: Edit `config/config.toml`
5. Run: `go run http/http_main.go` (for HTTP server) and `go run writer/writer_main.go` (for UDP data processing)

### Required External Files

GoSimView requires additional files from the original SimView package to function properly. These files are not included in this repository for legal reasons.

#### How to Obtain
1. Download the original SimView package from: [https://www.overtake.gg/downloads/simview.35249/](https://www.overtake.gg/downloads/simview.35249/)
2. Extract the package contents
3. Copy the following files to your GoSimView installation:
   - `config/` (configuration file)
   - `html/` directory (web UI files)
   - `data/` directory (database schema)

#### Database Setup
1. Create a MySQL database
2. Import the SQL schema from the original SimView package
3. Update `config/config.toml` with your database credentials

## License

GoSimView is dual-licensed:

1. **AGPLv3** - For open-source use. Source code must be made available when used over a network.
2. **Commercial License** - For proprietary use. No open-source requirements. Contact Kagurazakayukari for details.

See LICENSE.md for full license information.

## Disclaimer

GoSimView is an independent project not affiliated with the original SimView developers or Kunos Simulazioni. It's a reverse-engineered implementation based on publicly available information.

## Contributing

Contributions are welcome! Submit issues, feature requests, or pull requests.

## Contact

For commercial licensing or support: Kagurazakayukari

© 2026 Kagurazakayukari. All rights reserved.