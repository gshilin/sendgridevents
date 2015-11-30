package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gshilin/sendgridevents/Godeps/_workspace/src/github.com/gorilla/mux"
	_ "github.com/gshilin/sendgridevents/Godeps/_workspace/src/github.com/joho/godotenv/autoload"
	_ "github.com/gshilin/sendgridevents/Godeps/_workspace/src/github.com/lib/pq"
	"github.com/gshilin/sendgridevents/Godeps/_workspace/src/github.com/yvasiyarov/gorelic"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

type Event struct {
	ID          uint
	CreatedAt   time.Time
	UpdatedAt   time.Time

	Event       string
	Email       string
	Category    string `json:"category"`
	Timestamp   int64
	Happened_at time.Time
	Url         string
	SmtpId      string `json:"smtp-id"`
	SgMessageId string `json:"sg_message_id"`
	IP          string `json:"ip"`
	UserAgent   string `json:"useragent"`
}

type Events []Event

var (
	db     *sql.DB
	err    interface{}
	res sql.Result
	open_event_update, click_event_update *sql.Stmt
	chanDB chan (Event)
	quitDB chan (int)
)

const numOfUpdates = 20
const numOfEventsBuffer = 100

func main() {
	// prepare NewRelic
	configureNewRelic()

	// prepare DB
	db, err = prepareDB()
	if err != nil {
		return
	}

	defer closeDB(db)

	chanDB = make(chan Event, numOfEventsBuffer)
	quitDB = make(chan int)
	for i := 0; i < numOfUpdates; i++ {
		go updateDB()
	}

	r := mux.NewRouter()
	// We handle only one request for now...
	r.HandleFunc("/api/sendgrid_event", processEvent).Methods("POST")

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

	db.SetMaxOpenConns(numOfUpdates)
	db.SetMaxIdleConns(numOfUpdates)

	open_event_update, err = db.Prepare("UPDATE email_subscriptions SET opened_at = $1 WHERE email = $2")
	if err != nil {
		log.Fatalf("Prepare open_event_update error: %v\n", err)
		return
	}
	click_event_update, err = db.Prepare("UPDATE email_subscriptions SET (clicked_at, last_clicked_url) = ($1, $2) WHERE email = $3")
	if err != nil {
		log.Fatalf("Prepare click_event_update error: %v\n", err)
		return
	}

	return
}

func closeDB(db *sql.DB) {
	quitDB <- 0
	open_event_update.Close()
	click_event_update.Close()
	db.Close()
}

func processEvent(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Println("readall error:", err)
		return
	}
	req.Body.Close()

	events := Events{}
	if err := json.Unmarshal(body, &events); err != nil {
		fmt.Println("marshal error:", err)
		return
	}

	for _, event := range events {
		chanDB <- event
	}
}

func updateDB() {
	for {
		select {
		case <-quitDB:
			return
		case event := <-chanDB:
			email := event.Email
			timestamp := event.Timestamp

			if email == "" || timestamp == 0 {
				return
			}

			unixDate := time.Unix(timestamp, 0)
			occurred_at := unixDate.Format(time.RFC3339)
			url := event.Url

			switch event.Event {
			case "open":
				// q := fmt.Sprintf("UPDATE email_subscriptions SET opened_at = '%s' WHERE email = '%s'", occurred_at, email)
				_, err = open_event_update.Exec(occurred_at, email)
				if err != nil {
					log.Fatalf("Unable to register open event: %v\n", err)
				}
			case "click":
				clicked_url := url[0:min(len(url) - 1, 254)]
				// q := fmt.Sprintf("UPDATE email_subscriptions SET (clicked_at, last_clicked_url) = ('%s', '%s') WHERE email = '%s'", occurred_at, clicked_url, email)
				_, err = click_event_update.Exec(occurred_at, clicked_url, email)
				if err != nil {
					log.Fatalf("Unable to register click event: %v\n", err)
				}
			}

			now := time.Now().Format(time.RFC3339)
			q := fmt.Sprintf(
				"INSERT INTO sendgrid_events (created_at, updated_at, email, category, smtp_id, sg_message_id, ip, useragent, happened_at, event, url) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
				now, now, email, event.Category, event.SmtpId, event.SgMessageId, event.IP, event.UserAgent, occurred_at, event.Event, url)
			res, err = db.Exec(q)
			if err != nil {
				log.Fatalf("Unable to sendgrid_event: %v\n", err)
			}
			rowCount, err := res.RowsAffected()
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("inserted %d rows", rowCount)
		}
	}
}

func min(a, b int) int {
	if a <= b {
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
