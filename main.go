package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const (
	GET_USERS    = "https://jsonplaceholder.typicode.com/users"
	GET_POSTS    = "https://jsonplaceholder.typicode.com/posts"
	GET_COMMENTS = "https://jsonplaceholder.typicode.com/comments"
	GET_ALBUMS   = "https://jsonplaceholder.typicode.com/albums"
	GET_PHOTOS   = "https://jsonplaceholder.typicode.com/photos"
	GET_TODOS    = "https://jsonplaceholder.typicode.com/todos"
)

func FetchData(endpoint string, ch chan<- []byte, errCh chan<- error, wg *sync.WaitGroup) {
	start := time.Now()
	defer wg.Done()

	client := http.Client{
		Timeout: time.Second * 2,
	}

	res, err := client.Get(endpoint)
	if err != nil {
		errCh <- fmt.Errorf("failed to fetch %s: %w", endpoint, err)
		return
	}
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		errCh <- fmt.Errorf("failed to read response from %s: %w", endpoint, err)
		return
	}

	ch <- data

	fmt.Printf("[%s] took %v\n", endpoint, time.Since(start))
}

func main() {
	/*
		1. Consumir os endpoints da API JSONPlaceholder em paralelo, com goroutines separadas,
			 cada uma enviando os dados para canais individuais. Aplicar timeouts e preparar os
			 dados para a próxima etapa (pipeline/parsing).
	*/
	endpoints := []string{GET_USERS, GET_POSTS, GET_COMMENTS, GET_ALBUMS, GET_PHOTOS, GET_TODOS}
	var wg sync.WaitGroup
	ch := make(chan []byte, len(endpoints))
	errCh := make(chan error, len(endpoints))

	for _, e := range endpoints {
		wg.Add(1)
		go FetchData(e, ch, errCh, &wg)
	}

	go func() {
		wg.Wait()
		close(ch)
		close(errCh)
	}()

	for data := range ch {
		fmt.Printf("Received %d bytes\n", len(data))
	}

	for err := range errCh {
		fmt.Printf("Error: %v\n", err)
	}

	for data := range ch {
		fmt.Println("Data preview:", string(data[:10]))
	}
}
