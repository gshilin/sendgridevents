package main

import (
	"fmt"
	"time"
	"os"
	"github.com/gorilla/mux"
	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
	"net/http"
	"io/ioutil"
	"encoding/json"
)

var (
	config struct {
		DB gorm.DB
	}
	err interface{}
)

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/api/sendgrid_event", ProcessEvent).Methods("POST")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	config.DB, err = gorm.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Printf("DB connection error: %v\n", err)
		return
	}

	fmt.Println("SERVING on port", port)
	http.ListenAndServe(":" + port, r)
}

func ProcessEvent(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Println("readall error:", err)
		return
	}
	req.Body.Close()

	type Event struct {
		ID          uint `gorm:"primary_key"`
		CreatedAt   time.Time  `gorm:"column:created_at"`
		UpdatedAt   time.Time  `gorm:"column:updated_at"`

		Email       string `json:"email"`
		Timestamp   int64  `json:"timestamp" sql:"-"`
		Happened_at time.Time  `gorm:"column:happened_at"`
		Event       string `json:"event"`
		Url         string `json:"url"`
	}
	type Events []Event

	events := Events{}
	if err := json.Unmarshal(body, &events); err != nil {
		fmt.Println("marshal error:", err)
		return
	}

	type EmailSubscription struct {
	}
	type EmailSubscriptionOpen struct {
		OpenedAt string `gorm:opened_at`
	}
	type EmailSubscriptionClick struct {
		ClickedAt      string `gorm:clicked_at`
		LastClickedUrl string `gorm:last_clicked_url`
	}

	for _, event := range events {
		email := event.Email
		timestamp := event.Timestamp
		status := event.Event

		if email == "" || timestamp == 0 {
			continue
		}

		unixDate := time.Unix(timestamp, 0)
		occured_at := unixDate.Format(time.RFC3339)

		switch status {
		case "open":
			config.DB.Debug().Table("email_subscriptions").Where("email = ?", email).UpdateColumn("opened_at", occured_at)
		case "click":
			url := event.Url
			clicked_url := url[0:min(len(url) - 1, 254)]
			config.DB.Debug().Table("email_subscriptions").Where("email = ?", email).
			UpdateColumns(map[string]interface{}{"clicked_at": occured_at, "last_clicked_url": clicked_url})
		}
		event.Happened_at = unixDate
		config.DB.Debug().Table("sendgrid_events").Create(&event)
	}
}

func min(a, b int) int {
	if (a <= b) {
		return a
	} else {
		return b
	}
}
