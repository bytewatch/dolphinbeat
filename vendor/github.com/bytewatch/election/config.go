package election

type Config struct {
	ZkHosts []string
	ZkPath  string
	Lease   int
	Logger  Logger
}
