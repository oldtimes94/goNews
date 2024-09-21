package memdb

import (
	"GoNews/pkg/storage"
)

type Store struct {
	MemDB []storage.NewsPost
}

func New() *Store {
	return &Store{make([]storage.NewsPost, 0, 50)}
}

func (s *Store) Posts(num int) ([]storage.NewsPost, error) {
	if num > len(s.MemDB) {
		return s.MemDB, nil
	}

	return s.MemDB[len(s.MemDB)-num:], nil

}

func (s *Store) AddPosts(posts []storage.NewsPost) error {
	s.MemDB = append(s.MemDB, posts...)
	return nil
}
