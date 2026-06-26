package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
)

func healthHandler(svc string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		log.Printf(`{"level":"info","service":"%s","event":"request","path":"/health"}`, svc)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": svc})
	}
}

func main() {
	svc := os.Getenv("SERVICE_NAME")
	if svc == "" {
		svc = "game-integrity"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler(svc))
	log.Printf(`{"level":"info","service":"%s","event":"startup"}`, svc)
	log.Fatal(http.ListenAndServe(":8080", mux))
}
