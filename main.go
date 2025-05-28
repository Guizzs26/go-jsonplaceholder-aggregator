package main

import (
	"encoding/json"
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

	requestTimeout = 2 * time.Second
)

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
	ID            int    `json:"id"`
	Title         string `json:"title"`
	Url           string `json:"url"`
	ThumbanailUrl string `json:"thumbnailUrl"`
	AlbumID       int    `json:"albumId"`
}

type Todo struct {
	ID        int    `json:"id"`
	Title     string `json:"title"`
	Completed bool   `json:"completed"`
	UserID    int    `json:"userId"`
}

type ParsedData struct {
	Data     any
	Endpoint string
	Type     string
}

type RawResponse struct {
	Endpoint string
	Data     []byte
}

func parseData[T any](rawData RawResponse, parsedCh chan<- ParsedData, errCh chan<- error) {
	var data []T

	if err := json.Unmarshal(rawData.Data, &data); err != nil {
		errCh <- fmt.Errorf("failed to parse posts: %w", err)
	}

	parsedCh <- ParsedData{
		Data:     data,
		Endpoint: rawData.Endpoint,
		Type:     getType(rawData.Endpoint),
	}
}

func getType(endpoint string) string {
	switch endpoint {
	case GET_USERS:
		return "user"
	case GET_POSTS:
		return "post"
	case GET_COMMENTS:
		return "comment"
	case GET_ALBUMS:
		return "album"
	case GET_PHOTOS:
		return "photo"
	case GET_TODOS:
		return "todo"
	default:
		return "unknown"
	}
}

func fetchData(endpoint string, ch chan<- RawResponse, errCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	client := http.Client{
		Timeout: requestTimeout,
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

	ch <- RawResponse{
		Endpoint: endpoint,
		Data:     data,
	}
}

func main() {
	var wg sync.WaitGroup
	var parseWg sync.WaitGroup

	endpoints := []string{GET_USERS, GET_POSTS, GET_COMMENTS, GET_ALBUMS, GET_PHOTOS, GET_TODOS}

	rawCh := make(chan RawResponse, len(endpoints))
	parsedCh := make(chan ParsedData, len(endpoints))
	errCh := make(chan error, len(endpoints))

	// fetch stage
	wg.Add(len(endpoints))
	for _, e := range endpoints {
		go fetchData(e, rawCh, errCh, &wg)
	}
	go func() {
		wg.Wait()
		close(rawCh)
	}()

	parseWg.Add(1)
	go func() {
		defer parseWg.Done()
		for raw := range rawCh {
			switch raw.Endpoint {
			case GET_USERS:
				parseData[User](raw, parsedCh, errCh)
			case GET_POSTS:
				parseData[Post](raw, parsedCh, errCh)
			case GET_COMMENTS:
				parseData[Comment](raw, parsedCh, errCh)
			case GET_ALBUMS:
				parseData[Album](raw, parsedCh, errCh)
			case GET_PHOTOS:
				parseData[Photo](raw, parsedCh, errCh)
			case GET_TODOS:
				parseData[Todo](raw, parsedCh, errCh)
			}
		}
	}()
	// parse stage
	go func() {
		parseWg.Wait()
		close(parsedCh)
		close(errCh)
	}()

	// Consuming parsed data
	for parsed := range parsedCh {
		switch v := parsed.Data.(type) {
		case []User:
			for _, user := range v {
				fmt.Printf("[USER]: %s (ID: %d)\n", user.Name, user.ID)
			}
		case []Post:
			for _, post := range v {
				fmt.Printf("[POST]: %q (UserID: %d)\n", post.Title, post.UserID)
			}
		case []Comment:
			for _, comment := range v {
				fmt.Printf("[COMMENT]: %q (PostID: %d)\n", comment.Body, comment.PostID)
			}
		case []Album:
			for _, album := range v {
				fmt.Printf("[ALBUM]: %q (UserID: %d)\n", album.Title, album.UserID)
			}
		case []Photo:
			for _, photo := range v {
				fmt.Printf("[PHOTO]: %q (AlbumID: %d)\n", photo.Title, photo.AlbumID)
			}
		case []Todo:
			for _, todo := range v {
				fmt.Printf("[TODO]: %q (UserID: %d, Done: %v)\n", todo.Title, todo.UserID, todo.Completed)
			}
		default:
			fmt.Printf("Unknown type for endpoint %s\n", parsed.Endpoint)
		}
	}

	// consuming errors
	for err := range errCh {
		fmt.Printf("ERROR: %v\n", err)
	}
}
