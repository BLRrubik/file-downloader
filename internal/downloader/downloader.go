package downloader

import (
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

type Params struct {
	Size           int64
	SupportsResume bool
}

type Downloader struct {
	mu sync.Mutex

	maxConcurrentChunks int

	httpClient *http.Client
}

func NewDownloader(maxConcurrentChunks int, client *http.Client) *Downloader {
	return &Downloader{
		maxConcurrentChunks: maxConcurrentChunks,
		httpClient:          client,
	}
}

func (d *Downloader) Download(urls []string, savePath string) error {
	if err := os.MkdirAll(savePath, os.ModePerm); err != nil {
		return err
	}

	wg := sync.WaitGroup{}

	for _, url := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()

			if err := d.downloadURL(url, savePath); err != nil {
				fmt.Println("Ошибка при скачивании", url, ":", err)

				return
			}
		}(url)
	}

	wg.Wait()

	return nil
}

func (d *Downloader) downloadURL(url, savePath string) error {
	filename := getFileNameFromURL(url)
	progressFile := filename + ".progress"

	params, err := d.getFileParams(url)
	if err != nil {
		return fmt.Errorf("ошибка при получении параметров файла: %v", err)
	}

	state, err := StateFromFile(progressFile)
	if err != nil {
		switch {
		case os.IsNotExist(err):
			state = &DownloadState{
				File:             progressFile,
				URL:              url,
				TotalSize:        params.Size,
				ChunkSize:        chunkSize,
				TotalChunks:      int((params.Size + chunkSize - 1) / chunkSize),
				DownloadedChunks: make([]Chunk, int((params.Size+chunkSize-1)/chunkSize)),
			}
		default:
			return fmt.Errorf("ошибка при чтении состояния: %v", err)
		}
	}

	file, err := os.Create(savePath + "/" + filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if err = file.Truncate(params.Size); err != nil {
		return err
	}

	d.prepareChunks(state)

	chunkCh := make(chan int)

	wg := sync.WaitGroup{}
	for range d.maxConcurrentChunks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunkIdx := range chunkCh {
				chunk := state.DownloadedChunks[chunkIdx]

				if chunk.Completed {
					fmt.Println("Чанк", chunkIdx+1, "уже скачан, пропускаем...")

					continue
				}

				if err = d.downloadChunk(url, file, chunk.Start, chunk.End); err != nil {
					fmt.Println("Ошибка при скачивании чанка", chunkIdx+1, ":", err)

					continue
				}

				state.Lock()
				state.DownloadedChunks[chunkIdx].Completed = true
				state.Save()
				state.Unlock()

				fmt.Println("Чанк", chunkIdx+1, "скачан успешно")
			}
		}()
	}

	for i := range state.DownloadedChunks {
		chunkCh <- i
	}

	close(chunkCh)

	wg.Wait()

	return nil
}

func (d *Downloader) prepareChunks(state *DownloadState) {
	chunks := make([]Chunk, state.TotalChunks)
	for i := range state.TotalChunks {
		start := int64(i) * int64(state.ChunkSize)
		end := start + int64(state.ChunkSize) - 1
		if end > state.TotalSize-1 {
			end = state.TotalSize - 1
		}

		chunks[i] = Chunk{
			Start:     start,
			End:       end,
			Completed: state.DownloadedChunks[i].Completed,
		}
	}

	state.DownloadedChunks = chunks

	state.Save()
}

func (d *Downloader) downloadChunkWithRetries(url string, file *os.File, start, end int64) error {
	var err error
	for attempt := 0; attempt < maxRetries; attempt++ {
		err = d.downloadChunk(url, file, start, end)
		if err == nil {
			return nil
		}

		if attempt < maxRetries-1 {
			fmt.Printf("Ошибка, повтор через %v...\n", retryDelay)
			time.Sleep(retryDelay)
		}
	}

	return err
}

func (d *Downloader) downloadChunk(
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

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("ошибка чтения ответа: %v", err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	offset, err := file.Seek(startByte, io.SeekStart)
	if err != nil {
		return fmt.Errorf("ошибка перемещения указателя файла: %v", err)
	}

	if _, err = file.WriteAt(bytes, offset); err != nil {
		return fmt.Errorf("ошибка записи в файл: %v", err)
	}

	return nil
}

func (d *Downloader) getFileParams(url string) (*Params, error) {
	resp, err := d.httpClient.Head(url)
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

func getFileNameFromURL(url string) string {
	strs := strings.Split(url, "/")

	return strs[len(strs)-1]
}
