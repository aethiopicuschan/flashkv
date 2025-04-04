package store

import (
	"os"
	"path/filepath"
	"strings"
)

func expandPath(path string) (string, error) {
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		// "~" をホームディレクトリに置き換える
		path = filepath.Join(home, path[1:])
	}
	return path, nil
}
