package config

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

// Схема конфига
type Schema struct {
	RSS           []string      `json:"rss"`
	RequestPeriod time.Duration `json:"requestPeriod"`
}

func New() *Schema {
	cfgFile, err := os.ReadFile("config.json")
	if err != nil {
		log.Panicln(err)
	}

	config := new(Schema)
	err = json.Unmarshal(cfgFile, &config)
	if err != nil {
		log.Panicln(err)
	}

	return config
}
