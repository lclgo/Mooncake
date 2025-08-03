package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type RequestPayload struct {
	Command  string `json:"command"`
	FileName string `json:"filename"`
}

func GetReq(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// parse json
	var payload RequestPayload
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cmd := payload.Command
	fileName := payload.FileName
	log.Println("(whorwe)GetReq: 0 | fileName: ", fileName)
	switch cmd {
	case "register":
		log.Println("(whorwe)GetReq: 1 | register")
		// register file
		// store file
		// return fileID
	case "get":
		log.Println("(whorwe)GetReq: 2 | get")
		// get file
		// return file
	default:
		// error
	}
}
