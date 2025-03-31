package main

import (
	"log"
	"regexp"

	"github.com/caarlos0/env/v6"
	"github.com/joho/godotenv"
)

type Config struct {
	Port string `env:"PORT" envDefault:":11211"`
}

var config = Config{}

func init() {
	godotenv.Load()
	if err := env.Parse(&config); err != nil {
		log.Fatal(err)
	}
}

func GetConfig() Config {
	return config
}

func (c *Config) Valid() (valid bool) {
	re := regexp.MustCompile(`^:\d+$`)
	valid = re.MatchString(c.Port)
	return
}
