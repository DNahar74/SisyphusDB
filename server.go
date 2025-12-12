package main

import (
	"KV-Store/kv"
	"KV-Store/wal"
	"fmt"
	"net/http"
)

type Server struct {
	store *kv.Store
}

func NewServer() *Server {
	newStore, err := kv.NewKVStore("logs")
	if err != nil {
		panic(err)
	}
	return &Server{store: newStore}
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := r.URL.Query().Get("val")

	if key == "" || val == "" {
		http.Error(w, "Missing key/val", http.StatusBadRequest)
		return
	}
	// Store in table
	s.store.Put(key, val)

	// Write in logs
	er := s.store.Wal.Write(key, val, wal.CmdPut)
	if er != nil {
		fmt.Println("Error writing log: ", er)
	}

	_, err := fmt.Fprintf(w, "Success Put: %s in %s", key, val)
	fmt.Printf("put %s in %s\n", key, val) // server log
	if err != nil {
		return
	}
}
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "No key found", http.StatusBadRequest)
		return
	}
	val, ok := s.store.Get(key)

	if !ok {
		http.Error(w, "No key found", http.StatusBadRequest)
		fmt.Printf("Get %s: No Key Found\n", key) // server log
		return
	}
	fmt.Printf("Get %s: %s\n", key, val)
	_, err := fmt.Fprintf(w, "Success Get: %s -> %s", key, val)
	if err != nil {
		return
	}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "No key found", http.StatusBadRequest)
	}
	// Delete from table
	s.store.Delete(key)
	// Write in logs
	er := s.store.Wal.Write(key, "", wal.CmdDelete)
	if er != nil {
		fmt.Println("Error writing log: ", er)
	}

	fmt.Printf("Delete %s\n", key)
	_, err := fmt.Fprintf(w, "Success Delete: %s", key)
	if err != nil {
		return
	}
}
