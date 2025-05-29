package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

	httptimeout = time.Second * 2
)

var typeMap = map[string]string{
	GET_USERS:    "user",
	GET_POSTS:    "post",
	GET_COMMENTS: "comment",
	GET_ALBUMS:   "album",
	GET_PHOTOS:   "photo",
	GET_TODOS:    "todo",
}

type User struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type Post struct {
	ID     int    `json:"id"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	UserID int    `json:"userId"`
}

type Comment struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Body   string `json:"body"`
	PostID int    `json:"postId"`
}

type Album struct {
	ID     int    `json:"id"`
	Title  string `json:"title"`
	UserID int    `json:"userId"`
}

type Photo struct {
	ID           int    `json:"id"`
	Title        string `json:"title"`
	Url          string `json:"url"`
	ThumbnailUrl string `json:"thumbnailUrl"`
	AlbumID      int    `json:"albumId"`
}

type Todo struct {
	ID        int    `json:"id"`
	Title     string `json:"title"`
	Completed bool   `json:"completed"`
	UserID    int    `json:"userId"`
}

type RawResponse struct {
	Endpoint string
	Data     []byte
}

type ParsedData struct {
	Endpoint string
	Type     string
	Data     any
}

type StructuredError struct {
	Stage     string
	Endpoint  string
	Error     error
	Timestamp time.Time
}

func main() {
	endpoints := []string{GET_USERS, GET_POSTS, GET_COMMENTS, GET_ALBUMS, GET_PHOTOS, GET_TODOS}

	var wg sync.WaitGroup

	rawCh := make(chan RawResponse, len(endpoints))
	parsedCh := make(chan ParsedData, len(endpoints))
	errCh := make(chan StructuredError, len(endpoints)*2)

	go func() {
		wg.Wait()
		close(rawCh)
	}()

	wg.Add(len(endpoints))
	for _, e := range endpoints {
		go fetchData(e, "fetch", rawCh, errCh, &wg)
	}

	processed := 0
	totalExpected := len(endpoints)
	for processed < totalExpected {
		select {
		case raw, ok := <-rawCh:
			if !ok {
				rawCh = nil
				continue
			}

			var result any
			var parseOk bool

			switch raw.Endpoint {
			case GET_USERS:
				result, parseOk = parseData[User](raw, errCh, "parse")
			case GET_POSTS:
				result, parseOk = parseData[Post](raw, errCh, "parse")
			case GET_COMMENTS:
				result, parseOk = parseData[Comment](raw, errCh, "parse")
			case GET_ALBUMS:
				result, parseOk = parseData[Album](raw, errCh, "parse")
			case GET_PHOTOS:
				result, parseOk = parseData[Photo](raw, errCh, "parse")
			case GET_TODOS:
				result, parseOk = parseData[Todo](raw, errCh, "parse")
			}

			if parseOk {
				parsedCh <- ParsedData{
					Endpoint: raw.Endpoint,
					Type:     getType(raw.Endpoint),
					Data:     result,
				}
			}
			processed++

		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			fmt.Printf("❌ [%s][%s] %v\n", err.Stage, err.Endpoint, err.Error)
			processed++
		}
	}

	close(errCh)
	close(parsedCh)

	for parsed := range parsedCh {
		fmt.Printf("Parsed %s: %T\n", parsed.Type, parsed.Data)
	}

	fmt.Println("🚀 All responses have been processed.")
}

func fetchData(endpoint, stage string, rawCh chan RawResponse, errCh chan StructuredError, wg *sync.WaitGroup) {
	start := time.Now()
	defer wg.Done()

	client := http.Client{
		Timeout: httptimeout,
	}

	resp, err := client.Get(endpoint)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) && urlErr.Timeout() {
			errCh <- StructuredError{
				Stage:     stage,
				Endpoint:  endpoint,
				Error:     err,
				Timestamp: time.Now(),
			}
			return
		}
		errCh <- StructuredError{
			Stage:     stage,
			Endpoint:  endpoint,
			Error:     err,
			Timestamp: time.Now(),
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		errCh <- StructuredError{
			Stage:     stage,
			Endpoint:  endpoint,
			Error:     fmt.Errorf("HTTP %d", resp.StatusCode),
			Timestamp: time.Now(),
		}
		return
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		errCh <- StructuredError{
			Stage:     stage,
			Endpoint:  endpoint,
			Error:     err,
			Timestamp: time.Now(),
		}
		return
	}
	fmt.Printf("📥 [%s] fetch took %v\n", endpoint, time.Since(start))

	rawCh <- RawResponse{
		Endpoint: endpoint,
		Data:     raw,
	}
}

func getType(endpoint string) string {
	if t, ok := typeMap[endpoint]; ok {
		return t
	}
	return "unknown"
}

func parseData[T any](raw RawResponse, errCh chan<- StructuredError, stage string) ([]T, bool) {
	var data []T

	if err := json.Unmarshal(raw.Data, &data); err != nil {
		errCh <- StructuredError{
			Stage:     stage,
			Endpoint:  raw.Endpoint,
			Error:     err,
			Timestamp: time.Now(),
		}
		return nil, false
	}

	return data, true
}
