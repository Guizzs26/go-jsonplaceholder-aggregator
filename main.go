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
	Endpoint string
	Data     any
	Type     string
}

type RawResponse struct {
	Endpoint string
	Data     []byte
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
	start := time.Now()
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

	fmt.Printf("[%s] took %v\n", endpoint, time.Since(start))
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
				var users []User
				if err := json.Unmarshal(raw.Data, &users); err == nil {
					parsedCh <- ParsedData{Endpoint: raw.Endpoint, Data: users, Type: getType(raw.Endpoint)}
				} else {
					errCh <- fmt.Errorf("failed to parse users: %w", err)
				}

			case GET_POSTS:
				var posts []Post
				if err := json.Unmarshal(raw.Data, &posts); err == nil {
					parsedCh <- ParsedData{Endpoint: raw.Endpoint, Data: posts, Type: getType(raw.Endpoint)}
				} else {
					errCh <- fmt.Errorf("failed to parse posts: %w", err)
				}

			case GET_COMMENTS:
				var comments []Comment
				if err := json.Unmarshal(raw.Data, &comments); err == nil {
					parsedCh <- ParsedData{Endpoint: raw.Endpoint, Data: comments, Type: getType(raw.Endpoint)}
				} else {
					errCh <- fmt.Errorf("failed to parse comments: %w", err)
				}

			case GET_ALBUMS:
				var albums []Album
				if err := json.Unmarshal(raw.Data, &albums); err == nil {
					parsedCh <- ParsedData{Endpoint: raw.Endpoint, Data: albums, Type: getType(raw.Endpoint)}
				} else {
					errCh <- fmt.Errorf("failed to parse albums: %w", err)
				}

			case GET_PHOTOS:
				var photos []Photo
				if err := json.Unmarshal(raw.Data, &photos); err == nil {
					parsedCh <- ParsedData{Endpoint: raw.Endpoint, Data: photos, Type: getType(raw.Endpoint)}
				} else {
					errCh <- fmt.Errorf("failed to parse photos: %w", err)
				}

			case GET_TODOS:
				var todos []Todo
				if err := json.Unmarshal(raw.Data, &todos); err == nil {
					parsedCh <- ParsedData{Endpoint: raw.Endpoint, Data: todos, Type: getType(raw.Endpoint)}
				} else {
					errCh <- fmt.Errorf("failed to parse todos: %w", err)
				}
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
