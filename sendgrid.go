package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"io/ioutil"

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
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Println("readall error:", err)
		return
	}
	var runes []interface{}
	err = json.Unmarshal(body, &runes)
	if err != nil {
		fmt.Println("marshal error:", err)
		return
	}
	fmt.Printf("%+v\n", runes)
}
