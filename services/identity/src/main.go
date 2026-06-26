package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

var (
	mu    sync.Mutex
	users = map[string]string{}
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	log.Printf(`{"level":"info","service":"identity","event":"request","path":"/health","correlationId":"%s"}`, r.Header.Get("X-Correlation-Id"))
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "identity"})
}

func registerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Username == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	mu.Lock()
	users[body.Username] = body.Password
	mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	log.Printf(`{"level":"info","service":"identity","event":"request","path":"/register","correlationId":"%s"}`, r.Header.Get("X-Correlation-Id"))
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "username": body.Username})
}

func whoamiHandler(w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("user")
	if username == "" {
		username = "unknown"
	}
	w.Header().Set("Content-Type", "application/json")
	log.Printf(`{"level":"info","service":"identity","event":"request","path":"/whoami","correlationId":"%s"}`, r.Header.Get("X-Correlation-Id"))
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "username": username})
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/register", registerHandler)
	mux.HandleFunc("/whoami", whoamiHandler)
	log.Printf(`{"level":"info","service":"identity","event":"startup"}`)
	log.Fatal(http.ListenAndServe(":8080", mux))
}
