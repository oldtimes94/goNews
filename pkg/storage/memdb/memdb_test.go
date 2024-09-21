package memdb

import (
	"GoNews/pkg/storage"
	"testing"
)

func TestStore_Posts(t *testing.T) {
	s := New()
	posts := []storage.NewsPost{
		{ID: 1, Title: "Post 1", Content: "Content 1"},
		{ID: 2, Title: "Post 2", Content: "Content 2"},
	}

	err := s.AddPosts(posts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tests := []struct {
		name string
		num  int
		want int
	}{
		{"all posts", 2, 2},
		{"one post", 1, 1},
		{"more than available", 3, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.Posts(tt.num)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tt.want {
				t.Errorf("expected %d posts, got %d", tt.want, len(got))
			}
		})
	}
}

func TestStore_AddPosts(t *testing.T) {
	s := New()
	posts := []storage.NewsPost{
		{ID: 1, Title: "Post 1", Content: "Content 1"},
		{ID: 2, Title: "Post 2", Content: "Content 2"},
	}

	err := s.AddPosts(posts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(s.MemDB) != 2 {
		t.Errorf("expected 2 posts in MemDB, got %d", len(s.MemDB))
	}

	newPosts := []storage.NewsPost{
		{ID: 3, Title: "Post 3", Content: "Content 3"},
	}

	err = s.AddPosts(newPosts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(s.MemDB) != 3 {
		t.Errorf("expected 3 posts in MemDB, got %d", len(s.MemDB))
	}
}
