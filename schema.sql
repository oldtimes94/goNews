type NewsPost struct {
	ID      int    // номер записи
	Title   string // заголовок публикации
	Content string // содержание публикации
	PubTime int64  // время публикации
	Link    string // ссылка на источник
}

CREATE TABLE news (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    pubtime BIGINT DEFAULT 0,
    link TEXT NOT NULL
);