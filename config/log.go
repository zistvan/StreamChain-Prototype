package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

var Log LogConfig

type LogConfig struct {
	Validation  bool
	Endorsement bool
	Ordering    bool
	FullCommit  bool
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
		Log = LogConfig{false, false, false, false}
		return err
	}

	Log = lc

	fmt.Printf("Log config loaded: %t\n", Log)

	return nil
}
