package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

func main() {
	r := mux.newRouter()
	r.HandleFunc("/api/sendgrid_event", ProcessEvent).Methods("POST")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.ListenAndServe(":" + port, r)
}

func ProcessEvent(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	fmt.Fprintln(rw, "event processed")
}
