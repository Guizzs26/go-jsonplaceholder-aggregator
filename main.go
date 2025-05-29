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

type AggregatedData struct {
	Users        map[int]User
	Posts        map[int][]Post
	Albums       map[int][]Album
	Todos        map[int][]Todo
	Comments     map[int][]Comment
	Photos       map[int][]Photo
	PostsByUser  map[int][]Post
	AlbumsByUser map[int][]Album
	mu           sync.RWMutex
}

func NewAggregatedData() *AggregatedData {
	return &AggregatedData{
		Users:        make(map[int]User),
		Posts:        make(map[int][]Post),
		Albums:       make(map[int][]Album),
		Todos:        make(map[int][]Todo),
		Comments:     make(map[int][]Comment),
		Photos:       make(map[int][]Photo),
		PostsByUser:  make(map[int][]Post),
		AlbumsByUser: make(map[int][]Album),
	}
}

func (ad *AggregatedData) AddUsers(users []User) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	for _, u := range users {
		ad.Users[u.ID] = u
	}
}

func (ad *AggregatedData) AddPosts(posts []Post) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	for _, p := range posts {
		ad.Posts[p.UserID] = append(ad.Posts[p.UserID], p)
		ad.PostsByUser[p.UserID] = append(ad.PostsByUser[p.UserID], p)
	}
}

func (ad *AggregatedData) AddAlbums(albums []Album) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	for _, a := range albums {
		ad.Albums[a.UserID] = append(ad.Albums[a.UserID], a)
		ad.AlbumsByUser[a.UserID] = append(ad.AlbumsByUser[a.UserID], a)
	}
}

func (ad *AggregatedData) AddTodos(todos []Todo) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	for _, t := range todos {
		ad.Todos[t.UserID] = append(ad.Todos[t.UserID], t)
	}
}

func (ad *AggregatedData) AddComments(comments []Comment) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	for _, c := range comments {
		ad.Comments[c.PostID] = append(ad.Comments[c.PostID], c)
	}
}

func (ad *AggregatedData) AddPhotos(photos []Photo) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	for _, p := range photos {
		ad.Photos[p.AlbumID] = append(ad.Photos[p.AlbumID], p)
	}
}

func (ad *AggregatedData) GetEnrichedUsers() []EnrichedUser {
	ad.mu.RLock()
	defer ad.mu.RUnlock()

	var enriched []EnrichedUser
	for _, user := range ad.Users {
		commentCount := 0
		for _, post := range ad.PostsByUser[user.ID] {
			commentCount += len(ad.Comments[post.ID])
		}

		photoCount := 0
		for _, album := range ad.AlbumsByUser[user.ID] {
			photoCount += len(ad.Photos[album.ID])
		}

		enriched = append(enriched, EnrichedUser{
			ID:           user.ID,
			Name:         user.Name,
			Email:        user.Email,
			PostCount:    len(ad.Posts[user.ID]),
			AlbumCount:   len(ad.Albums[user.ID]),
			TodoCount:    len(ad.Todos[user.ID]),
			CommentCount: commentCount,
			PhotoCount:   photoCount,
		})
	}
	return enriched
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
	aggregated := NewAggregatedData()

	rawCh := make(chan RawResponse, len(endpoints))
	parsedCh := make(chan ParsedData, len(endpoints))
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
	for range parseWorkers {
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
			aggregated.AddUsers(v)
		case []Post:
			aggregated.AddPosts(v)
		case []Comment:
			aggregated.AddComments(v)
		case []Album:
			aggregated.AddAlbums(v)
		case []Photo:
			aggregated.AddPhotos(v)
		case []Todo:
			aggregated.AddTodos(v)
		default:
			fmt.Printf("Unknown type for endpoint %s\n", parsed.Endpoint)
		}
	}

	enrichedUser := aggregated.GetEnrichedUsers()

	fmt.Println("===== Aggregated Data =====")
	for _, enriched := range enrichedUser {
		fmt.Printf("User: %-20s | Posts: %d | Comments: %d | Albums: %d | Photos: %d | Todos: %d\n",
			enriched.Name, enriched.PostCount, enriched.CommentCount, enriched.AlbumCount, enriched.PhotoCount, enriched.TodoCount)
	}

	errWg.Wait()
}
