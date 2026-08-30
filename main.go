package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	chunkSize  = 100 << 10
	maxRetries = 3
	retryDelay = 2 * time.Second
)

type DownloadState struct {
	File             string `json:"-"`
	URL              string `json:"url"`
	TotalSize        int64  `json:"total_size"`
	ChunkSize        int    `json:"chunk_size"`
	TotalChunks      int    `json:"total_chunks"`
	DownloadedChunks []bool `json:"downloaded_chunks"`
}

func (s *DownloadState) Save() {
	data, _ := json.MarshalIndent(s, "", "  ")
	os.WriteFile(s.File, data, 0644)
}

func main() {
	// os.Args для команды: ./downloader ./downloads http://example.com/file.zip
	// os.Args[0] = "./downloader"
	// os.Args[1] = "./downloads"
	// os.Args[2] = "http://example.com/file.zip"

	if len(os.Args) < 3 {
		fmt.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	savePath := os.Args[1]
	urls := os.Args[2:]

	fmt.Println("Директория для сохранения:", savePath)

	wg := sync.WaitGroup{}
	wg.Add(len(urls))
	for _, url := range urls {
		go func(url string) {
			defer wg.Done()
			if err := downloadFile(url, savePath); err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Файл сохранён: ", getFileNameFromURL(url))
			}
		}(url)
	}

	wg.Wait()
}

func downloadFile(url, savePath string) error {
	if err := os.MkdirAll(savePath, os.ModePerm); err != nil {
		return err
	}

	params, err := getFileParams(url)
	if err != nil {
		return err
	}
	totalChunks := (params.Size + chunkSize - 1) / chunkSize

	filename := getFileNameFromURL(url)
	progressFile := filename + ".progress"

	var state DownloadState
	_, err = os.Stat(progressFile)
	switch {
	case err == nil:
		data, _ := os.ReadFile(progressFile)
		json.Unmarshal(data, &state)

		state.File = progressFile
	case os.IsNotExist(err):
		state = DownloadState{
			File:             progressFile,
			URL:              url,
			TotalSize:        params.Size,
			ChunkSize:        chunkSize,
			TotalChunks:      int(totalChunks),
			DownloadedChunks: make([]bool, totalChunks),
		}

		state.Save()
	default:
		return fmt.Errorf("ошибка проверки прогресса: %v", err)
	}

	fmt.Println("Файл:", filename)
	fmt.Println("\tРазмер:", params.Size)
	fmt.Println("\tДокачка:", params.SupportsResume)

	file, err := os.Create(savePath + "/" + filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if err = file.Truncate(params.Size); err != nil {
		return err
	}

	for i := range totalChunks {
		fmt.Printf("Чанк %d", i+1)
		if state.DownloadedChunks[i] {
			fmt.Println(" - уже загружен, пропускаем")
			continue
		}

		start := i * chunkSize
		end := start + chunkSize - 1
		if end > params.Size-1 {
			end = params.Size - 1
		}

		for attempt := 0; attempt < maxRetries; attempt++ {
			err = downloadChunk(url, file, start, end)
			if err == nil {
				break
			}

			if attempt < maxRetries-1 {
				fmt.Printf("Ошибка, повтор через %v...\n", retryDelay)
				time.Sleep(retryDelay)
			}
		}
		if err != nil {
			return fmt.Errorf("не удалось скачать чанк %d: %v", i+1, err)
		}

		state.DownloadedChunks[i] = true

		state.Save()

		fmt.Println(" - загружен")
	}

	return err
}

func getFileNameFromURL(url string) string {
	strs := strings.Split(url, "/")

	return strs[len(strs)-1]
}

type Params struct {
	Size           int64
	SupportsResume bool
}

func getFileParams(url string) (*Params, error) {
	client := &http.Client{Timeout: 30 * time.Second}

	resp, err := client.Head(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Размер файла
	contentLength := resp.Header.Get("Content-Length")
	size, _ := strconv.ParseInt(contentLength, 10, 64)

	// Поддержка докачки
	acceptRanges := resp.Header.Get("Accept-Ranges")
	supportsResume := acceptRanges == "bytes"

	return &Params{
		Size:           size,
		SupportsResume: supportsResume,
	}, err
}

func downloadChunk(
	url string,
	file *os.File,
	startByte, endByte int64,
) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("ошибка создания запроса: %v", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", startByte, endByte))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка выполнения запроса: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("сервер вернул %d", resp.StatusCode)
	}

	_, err = file.Seek(startByte, io.SeekStart)
	if err != nil {
		return fmt.Errorf("ошибка перемещения указателя файла: %v", err)
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("ошибка записи в файл: %v", err)
	}

	return nil
}
