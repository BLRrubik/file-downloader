package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"golang.org/x/sync/errgroup"
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

	httpClient  *http.Client
	pBarManager *mpb.Progress
}

func NewDownloader(
	maxConcurrentChunks int,
	client *http.Client,
	pBarManager *mpb.Progress,
) *Downloader {
	return &Downloader{
		maxConcurrentChunks: maxConcurrentChunks,
		httpClient:          client,
		pBarManager:         pBarManager,
	}
}

func (d *Downloader) Download(ctx context.Context, urls []string, savePath string) error {
	if err := os.MkdirAll(savePath, os.ModePerm); err != nil {
		return err
	}

	var mu sync.Mutex
	var resErr error

	wg := sync.WaitGroup{}

	for _, url := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()

			if err := d.downloadURL(ctx, url, savePath); err != nil {
				mu.Lock()
				resErr = errors.Join(resErr, err)
				mu.Unlock()
			}
		}(url)
	}

	wg.Wait()

	return resErr
}

func (d *Downloader) downloadURL(ctx context.Context, url, savePath string) error {
	filename := getFileNameFromURL(url)
	progressFile := filename + ".progress"

	params, err := d.getFileParams(url)
	if err != nil {
		return fmt.Errorf("%s: ошибка при получении параметров файла: %w", filename, err)
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
			return fmt.Errorf("%s: ошибка при чтении состояния: %w", filename, err)
		}
	}

	file, err := os.Create(savePath + "/" + filename)
	if err != nil {
		return fmt.Errorf("%s: %w", filename, err)
	}
	defer file.Close()

	if err = file.Truncate(params.Size); err != nil {
		return fmt.Errorf("%s: %w", filename, err)
	}

	d.prepareChunks(state)

	bar := d.prepareBar(state, params, filename)

	g, ctx := errgroup.WithContext(ctx)
	chunkCh := make(chan int)

	for range d.maxConcurrentChunks {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case chunkIdx, ok := <-chunkCh:
					if !ok {
						return nil
					}

					chunk := state.DownloadedChunks[chunkIdx]

					if chunk.Completed {
						continue
					}

					if err := d.downloadChunkWithRetries(ctx, url, file, chunk.Start, chunk.End); err != nil {
						return fmt.Errorf("чанк %d: %w", chunkIdx, err)
					}

					state.Lock()
					state.DownloadedChunks[chunkIdx].Completed = true
					state.Unlock()

					bar.IncrBy(int(chunk.End - chunk.Start + 1))
				}
			}
		})
	}

	g.Go(func() error {
		defer close(chunkCh)

		for i := range state.DownloadedChunks {
			select {
			case chunkCh <- i:
			case <-ctx.Done():
				return nil
			}
		}

		return nil
	})

	waitErr := g.Wait()

	state.Lock()
	state.Save()
	state.Unlock()

	if waitErr != nil {
		bar.Abort(false)

		return fmt.Errorf("%s: ошибка при скачивании: %w", filename, waitErr)
	}

	if d.checkFileFinished(state) {
		_ = os.Remove(progressFile)
	}

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

func (d *Downloader) prepareBar(state *DownloadState, params *Params, filename string) *mpb.Bar {
	var alreadyDone int64
	for _, chunk := range state.DownloadedChunks {
		if chunk.Completed {
			alreadyDone += chunk.End - chunk.Start + 1
		}
	}

	bar := d.pBarManager.AddBar(
		params.Size,
		mpb.PrependDecorators(decor.Name(filename)),
		mpb.AppendDecorators(decor.OnAbort(decor.Percentage(), "ошибка")),
	)
	bar.SetCurrent(alreadyDone)

	return bar
}

func (d *Downloader) downloadChunkWithRetries(ctx context.Context, url string, file *os.File, start, end int64) error {
	var err error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err = d.downloadChunk(ctx, url, file, start, end)
		if err == nil {
			return nil
		}

		if attempt < maxRetries-1 {
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return err
}

func (d *Downloader) downloadChunk(
	ctx context.Context,
	url string,
	file *os.File,
	startByte, endByte int64,
) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
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

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("сервер вернул %d", resp.StatusCode)
	}

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

func (d *Downloader) checkFileFinished(state *DownloadState) bool {
	state.Lock()
	defer state.Unlock()

	for _, chunk := range state.DownloadedChunks {
		if !chunk.Completed {
			return false
		}
	}

	return true
}

func getFileNameFromURL(url string) string {
	strs := strings.Split(url, "/")

	return strs[len(strs)-1]
}
