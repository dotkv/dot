package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

type residentMemory struct {
	Current uint64 `json:"current_bytes"`
	Peak    uint64 `json:"peak_bytes"`
}

func readRSS() (residentMemory, error) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return residentMemory{}, err
	}
	var result residentMemory
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || (fields[0] != "VmRSS:" && fields[0] != "VmHWM:") {
			continue
		}
		n, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil || fields[2] != "kB" {
			return result, fmt.Errorf("invalid resident memory field %q", line)
		}
		if fields[0] == "VmRSS:" {
			result.Current = n * 1024
		} else {
			result.Peak = n * 1024
		}
	}
	if result.Current == 0 || result.Peak == 0 {
		return result, fmt.Errorf("VmRSS or VmHWM unavailable")
	}
	return result, nil
}

type diskSize struct {
	FileBytes      uint64 `json:"file_bytes"`
	AllocatedBytes uint64 `json:"allocated_bytes"`
}

func diskUsage(path string) (diskSize, error) {
	var result diskSize
	err := filepath.WalkDir(path, func(path string, entry fs.DirEntry, err error) error {
		// Compaction may delete a file between reading its directory and stat.
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		result.FileBytes += uint64(info.Size())
		result.AllocatedBytes += uint64(info.Sys().(*syscall.Stat_t).Blocks) * 512
		return nil
	})
	return result, err
}
