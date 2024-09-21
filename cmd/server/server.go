package main

import (
	"GoNews/pkg/api"
	"GoNews/pkg/config"
	"GoNews/pkg/storage"
	"GoNews/pkg/storage/postgres"
	"GoNews/pkg/xmlHandler"
	"log"
	"net/http"
)

// Сервер GoNews.
type server struct {
	db  storage.Interface
	api *api.API
}

func main() {
	// Создаём объект сервера.
	var srv server

	cfg := config.New()

	if len(cfg.RSS) == 0 {
		log.Panicln("no rss provided")
	}

	posts := make(chan storage.NewsPost, 3)
	errors := make(chan error)

	// Создаём объекты баз данных.

	// Реляционная БД PostgreSQL.
	db, err := postgres.New("postgres://postgres:123456@localhost:5432/postgres")
	if err != nil {
		log.Fatal(err)
	}

	// Инициализируем хранилище сервера конкретной БД.
	srv.db = db

	//Запуск горутины для буферизации новостей
	go storage.NewsBuffer(posts, errors, srv.db)

	//Запуск горутин для обхода RSS потоков по заданному интервалу
	for _, url := range cfg.RSS {
		go xmlHandler.XMLHandler(url, cfg.RequestPeriod, posts, errors)
	}

	// Создаём объект API и регистрируем обработчики.
	srv.api = api.New(srv.db)

	// Запускаем веб-сервер на порту 8080 на всех интерфейсах.
	// Предаём серверу маршрутизатор запросов,
	// поэтому сервер будет все запросы отправлять на маршрутизатор.
	// Маршрутизатор будет выбирать нужный обработчик.
	http.ListenAndServe(":8080", srv.api.Router())
}
