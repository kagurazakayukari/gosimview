module gosimview

replace gosimview/kn5conv => ./kn5conv

go 1.25.3

require (
	github.com/go-ini/ini v1.67.0
	github.com/go-sql-driver/mysql v1.9.3
	github.com/gorilla/websocket v1.5.3
	github.com/pelletier/go-toml/v2 v2.2.4
	github.com/sirupsen/logrus v1.9.3
	go.etcd.io/bbolt v1.4.3
	golang.org/x/text v0.31.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	golang.org/x/sys v0.29.0 // indirect
)
