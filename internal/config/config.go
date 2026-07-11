package config

import "os"

const DefaultConcurrency = 4

func DatabasePath() string {
	if path := os.Getenv("AQS_DB"); path != "" {
		return path
	}
	return "aqs.db"
}
