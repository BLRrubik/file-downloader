package downloader

import (
	"encoding/json"
	"os"
	"sync"
)

type Chunk struct {
	Completed bool
	Start     int64
	End       int64
}

type DownloadState struct {
	File             string  `json:"-"`
	URL              string  `json:"url"`
	TotalSize        int64   `json:"total_size"`
	ChunkSize        int     `json:"chunk_size"`
	TotalChunks      int     `json:"total_chunks"`
	DownloadedChunks []Chunk `json:"downloaded_chunks"`

	mu sync.Mutex
}

func (s *DownloadState) Save() {
	data, _ := json.MarshalIndent(s, "", "  ")
	os.WriteFile(s.File, data, 0644)
}

func (s *DownloadState) Lock() {
	s.mu.Lock()
}

func (s *DownloadState) Unlock() {
	s.mu.Unlock()
}

func StateFromFile(file string) (*DownloadState, error) {
	var state DownloadState
	_, err := os.Stat(file)
	if err != nil {
		return nil, err
	}

	data, _ := os.ReadFile(file)
	json.Unmarshal(data, &state)

	state.File = file

	return &state, nil
}
