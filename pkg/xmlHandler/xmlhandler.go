package xmlHandler

import (
	"GoNews/pkg/storage"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"time"
)

const layout = "Mon, 02 Jan 2006 15:04:05 MST"

type RSS struct {
	XMLName xml.Name `xml:"rss"`
	Text    string   `xml:",chardata"`
	Version string   `xml:"version,attr"`
	Dc      string   `xml:"dc,attr"`
	Channel struct {
		Text           string `xml:",chardata"`
		Title          string `xml:"title"`
		Link           string `xml:"link"`
		Description    string `xml:"description"`
		Language       string `xml:"language"`
		ManagingEditor string `xml:"managingEditor"`
		Generator      string `xml:"generator"`
		PubDate        string `xml:"pubDate"`
		Image          struct {
			Text  string `xml:",chardata"`
			Link  string `xml:"link"`
			URL   string `xml:"url"`
			Title string `xml:"title"`
		} `xml:"image"`
		Item []struct {
			Text  string `xml:",chardata"`
			Title string `xml:"title"`
			Guid  struct {
				Text        string `xml:",chardata"`
				IsPermaLink string `xml:"isPermaLink,attr"`
			} `xml:"guid"`
			Link        string   `xml:"link"`
			Description string   `xml:"description"`
			PubDate     string   `xml:"pubDate"`
			Creator     string   `xml:"creator"`
			Category    []string `xml:"category"`
		} `xml:"item"`
	} `xml:"channel"`
}

func getContent(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GET error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Status error: %v", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Read body: %v", err)
	}

	return data, nil
}

func New(data []byte) *RSS {
	rss := new(RSS)
	err := xml.Unmarshal(data, rss)
	if err != nil {
		fmt.Printf("error: %v", err)
		return nil
	}

	return rss

}

func XMLHandler(url string, interval time.Duration, post chan storage.NewsPost, errors chan error) {
	for {
		select {
		case <-time.After(interval * time.Minute):
			content, err := getContent(url)
			if err != nil {
				errors <- err
				continue
			}

			rss := New(content)
			for _, item := range rss.Channel.Item {
				newspost := storage.New(item.Title, item.Description, item.Link, toTimestamp(item.PubDate))
				post <- newspost
			}

		}
	}
}

func toTimestamp(pubdate string) int64 {
	t, err := time.Parse(layout, pubdate)
	if err != nil {
		return 0
	}
	return t.Unix()
}
