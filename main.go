package main

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

	filename := getFileNameFromURL(url)

	fmt.Println("Файл:", filename)
	fmt.Println("\tРазмер:", params.Size)
	fmt.Println("\tДокачка:", params.SupportsResume)
	fmt.Println("Начало загрузки целиком...")

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("сервер вернул %d", resp.StatusCode)
	}

	file, err := os.Create(savePath + "/" + filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)

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
