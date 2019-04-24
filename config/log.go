package config

import (
	"encoding/json"
	"errors"
	"os"
)

var Log LogConfig

type LogConfig struct {
	Endorsement bool
	Validation  bool
	Ordering    bool
}

func ReadConfig(path string) error {

	file, err := os.Open(path)

	if err != nil {
		return errors.New("No config file found")
	}

	defer file.Close()

	dec := json.NewDecoder(file)

	lc := LogConfig{}

	if err := dec.Decode(&lc); err != nil {
		Log = LogConfig{false, false, false}
		return err
	}

	Log = lc

	return nil
}
