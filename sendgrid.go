package main

import (
	_ "encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/api/sendgrid_event", ProcessEvent).Methods("POST")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.ListenAndServe(":" + port, r)
}

func ProcessEvent(w http.ResponseWriter, req *http.Request) {
	form := req.ParseForm()
	log.Println(w, form)
}
