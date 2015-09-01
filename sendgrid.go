package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/julienschmidt/httprouter"
)

func main() {
	r := httprouter.New()
	r.POST("/api/sendgrid_event/", ProcessEvent)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.ListenAndServe(":" + port, r)
}

func ProcessEvent(rw http.ResponseWriter, r *http.Request, p httprouter.Params) {
	fmt.Fprintln(rw, "event processed")
}
