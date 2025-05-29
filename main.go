package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
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

	parseWorkers = 6

	requestTimeout = 2 * time.Second
)

var (
	usersMap    = make(map[int]User)
	postsMap    = make(map[int][]Post)
	albumsMap   = make(map[int][]Album)
	photosMap   = make(map[int][]Photo)
	commentsMap = make(map[int][]Comment)
	todosMap    = make(map[int][]Todo)
)

var typeMap = map[string]string{
	GET_USERS:    "user",
	GET_POSTS:    "post",
	GET_COMMENTS: "comment",
	GET_ALBUMS:   "album",
	GET_PHOTOS:   "photo",
	GET_TODOS:    "todo",
}

type EnrichedUser struct {
	ID           int
	Name         string
	Email        string
	PostCount    int
	AlbumCount   int
	TodoCount    int
	CommentCount int
	PhotoCount   int
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
	Data     any
	Endpoint string
	Type     string
}

type PipelineError struct {
	Stage     string
	Endpoint  string
	Error     error
	Timestamp time.Time
}

func parseData[T any](rawData RawResponse, parsedCh chan<- ParsedData, errCh chan<- PipelineError, stage string) {
	var data []T

	if err := json.Unmarshal(rawData.Data, &data); err != nil {
		errCh <- PipelineError{
			Stage:     stage,
			Endpoint:  rawData.Endpoint,
			Error:     err,
			Timestamp: time.Now(),
		}
		return
	}

	parsedCh <- ParsedData{
		Data:     data,
		Endpoint: rawData.Endpoint,
		Type:     getType(rawData.Endpoint),
	}
}

func getType(endpoint string) string {
	if t, ok := typeMap[endpoint]; ok {
		return t
	}

	return "unknown"
}

func fetchData(endpoint string, ch chan<- RawResponse, errCh chan<- PipelineError, wg *sync.WaitGroup, stage string) {
	defer wg.Done()

	client := http.Client{
		Timeout: requestTimeout,
	}

	res, err := client.Get(endpoint)
	if err != nil {
		errCh <- PipelineError{
			Stage:     stage,
			Endpoint:  endpoint,
			Error:     err,
			Timestamp: time.Now(),
		}
		return
	}
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		errCh <- PipelineError{
			Stage:     stage,
			Endpoint:  endpoint,
			Error:     err,
			Timestamp: time.Now(),
		}
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
	var errWg sync.WaitGroup

	endpoints := []string{GET_USERS, GET_POSTS, GET_COMMENTS, GET_ALBUMS, GET_PHOTOS, GET_TODOS}

	rawCh := make(chan RawResponse, len(endpoints))
	parsedCh := make(chan ParsedData, len(endpoints))
	enrichedCh := make(chan EnrichedUser, len(usersMap))
	errCh := make(chan PipelineError, len(endpoints)*2)

	// consuming errors
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		var fetchErr, parseErr int
		for err := range errCh {
			log.Printf("%s ERROR in %s: %v (at %v)",
				strings.ToUpper(err.Stage),
				err.Endpoint,
				err.Error,
				err.Timestamp.Format(time.RFC3339),
			)

			switch err.Stage {
			case "fetch":
				fetchErr++
			case "parse":
				parseErr++
			}
		}

		fmt.Printf("Total fetch errors: %d\n", fetchErr)
		fmt.Printf("Total parse errors: %d\n", parseErr)
	}()

	// fetch stage
	wg.Add(len(endpoints))
	for _, e := range endpoints {
		go fetchData(e, rawCh, errCh, &wg, "fetch")
	}
	go func() {
		wg.Wait()
		close(rawCh)
	}()

	// parse stage
	parseWg.Add(parseWorkers)
	for i := 0; i < parseWorkers; i++ {
		go func() {
			defer parseWg.Done()
			for raw := range rawCh {
				switch raw.Endpoint {
				case GET_USERS:
					parseData[User](raw, parsedCh, errCh, "parse")
				case GET_POSTS:
					parseData[Post](raw, parsedCh, errCh, "parse")
				case GET_COMMENTS:
					parseData[Comment](raw, parsedCh, errCh, "parse")
				case GET_ALBUMS:
					parseData[Album](raw, parsedCh, errCh, "parse")
				case GET_PHOTOS:
					parseData[Photo](raw, parsedCh, errCh, "parse")
				case GET_TODOS:
					parseData[Todo](raw, parsedCh, errCh, "parse")
				}
			}
		}()
	}
	go func() {
		parseWg.Wait()
		close(parsedCh)
		close(errCh)
	}()

	// Consuming parsed data
	for parsed := range parsedCh {
		switch v := parsed.Data.(type) {
		case []User:
			for _, u := range v {
				usersMap[u.ID] = u
			}
		case []Post:
			for _, p := range v {
				postsMap[p.UserID] = append(postsMap[p.UserID], p)
			}
		case []Comment:
			for _, c := range v {
				commentsMap[c.PostID] = append(commentsMap[c.PostID], c)
			}
		case []Album:
			for _, a := range v {
				albumsMap[a.UserID] = append(albumsMap[a.UserID], a)
			}
		case []Photo:
			for _, p := range v {
				photosMap[p.AlbumID] = append(photosMap[p.AlbumID], p)
			}
		case []Todo:
			for _, t := range v {
				todosMap[t.UserID] = append(todosMap[t.UserID], t)
			}
		default:
			fmt.Printf("Unknown type for endpoint %s\n", parsed.Endpoint)
		}
	}

	for _, user := range usersMap {
		go func(u User) {
			enriched := EnrichedUser{
				ID:         u.ID,
				Name:       u.Name,
				Email:      u.Email,
				PostCount:  len(postsMap[u.ID]),
				AlbumCount: len(albumsMap[u.ID]),
				TodoCount:  len(todosMap[u.ID]),
			}

			enrichedCh <- enriched
		}(user)
	}

	fmt.Println("===== Aggregated Data =====")
	for enriched := range enrichedCh {
		fmt.Printf("User: %-20s | Posts: %d | Albums: %d | Todos: %d\n",
			enriched.Name, enriched.PostCount, enriched.AlbumCount, enriched.TodoCount)
	}

	errWg.Wait()
}
