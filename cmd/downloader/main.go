package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"file-downloader/internal/downloader"

	"github.com/vbauerster/mpb/v8"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	savePath := os.Args[1]
	urls := os.Args[2:]

	pBarManager := mpb.New()

	client := &http.Client{Timeout: 30 * time.Second}
	d := downloader.NewDownloader(4, client, pBarManager)

	// Обработка сигнала прерывания
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		<-sigChan
		fmt.Println("\nПолучен сигнал прерывания, завершаем...")
		cancel()
	}()

	err := d.Download(ctx, urls, savePath)

	pBarManager.Wait()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
