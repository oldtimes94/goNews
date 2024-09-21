package storage

import (
	"log"
	"time"
)

// Post - публикация.

type NewsPost struct {
	ID      int    // номер записи
	Title   string // заголовок публикации
	Content string // содержание публикации
	PubTime int64  // время публикации
	Link    string // ссылка на источник
}

func New(title, content, link string, pubtime int64) NewsPost {
	return NewsPost{
		ID:      0,
		Title:   title,
		Content: content,
		PubTime: pubtime,
		Link:    link,
	}
}

func NewsBuffer(post chan NewsPost, errors chan error, db Interface) {
	posts := make([]NewsPost, 0, 50)

	for {
		select {
		case <-time.After(30 * time.Second):
			if len(posts) == 0 {
				continue
			}

			err := db.AddPosts(posts)
			if err != nil {
				log.Println(err)
				continue
			}

			posts = posts[:0]
		case msg := <-post:
			posts = append(posts, msg)
		case err := <-errors:
			log.Println(err)
		}
	}

}

// Interface задаёт контракт на работу с БД.
type Interface interface {
	Posts(num int) ([]NewsPost, error) // получение всех публикаций
	AddPosts(posts []NewsPost) error   // Пакетная вставка
}
