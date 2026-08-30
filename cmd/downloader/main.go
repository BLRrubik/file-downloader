package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"file-downloader/internal/downloader"
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

	client := &http.Client{Timeout: 30 * time.Second}
	d := downloader.NewDownloader(4, client)

	if err := d.Download(urls, savePath); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
