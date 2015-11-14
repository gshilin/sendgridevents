package main

import (
	"fmt"
	"time"
	"os"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"github.com/yvasiyarov/gorelic"
	"database/sql"
	"log"
	_ "github.com/joho/godotenv/autoload"
)

var (
	db *sql.DB
	clickPreparedStmt, openPreparedStmt *sql.Stmt
	err interface{}
)

func main() {
	// prepare NewRelic
	configureNewRelic()

	// prepare DB
	db, err = prepareDB()
	if err != nil {
		return
	}

	defer closeDB(db)

	r := mux.NewRouter()
	// We handle only one request for now...
	r.HandleFunc("/api/sendgrid_event", ProcessEvent).Methods("POST")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Println("SERVING on port", port)
	http.ListenAndServe(":" + port, r)
}

func prepareDB() (db *sql.DB, err error) {
	db, err = sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("DB connection error: %v\n", err)
		return
	}
	err = db.Ping() // really connect to db
	if err != nil {
		log.Fatalf("DB real connection error: %v\n", err)
		return
	}

	openPreparedStmt, err = db.Prepare("UPDATE email_subscriptions SET opened_at = $1 WHERE email = $2")
	if err != nil {
		log.Fatalf("Unable to prepare open statement: %v\n", err)
	}
	clickPreparedStmt, err = db.Prepare("UPDATE email_subscriptions SET (clicked_at, last_clicked_url) = ($1, $2) WHERE email = $3")
	if err != nil {
		log.Fatalf("Unable to prepare click statement: %v\n", err)
	}
	return
}

func closeDB(db *sql.DB) {
	fmt.Println("Closing DB")
	openPreparedStmt.Close()
	clickPreparedStmt.Close()
	db.Close()
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
			_, err = openPreparedStmt.Exec(occured_at, email)
			log.Println("Open :", email)
		case "click":
			url := event.Url
			clicked_url := url[0:min(len(url) - 1, 254)]
			_, err = clickPreparedStmt.Exec(occured_at, clicked_url, email)
			log.Println("Click:", email)
		}
		if err != nil {
			fmt.Println("update error:", err)
			return
		}

		event.Happened_at = unixDate
		//		config.DB.Debug().Table("sendgrid_events").Create(&event)
	}
}

func min(a, b int) int {
	if (a <= b) {
		return a
	} else {
		return b
	}
}

func configureNewRelic() {
	agent := gorelic.NewAgent()
	agent.Verbose = true
	agent.NewrelicLicense = os.Getenv("NEW_RELIC_LICENSE_KEY")
	agent.NewrelicName = "Go sendgrid events handler"
	agent.Run()
}
