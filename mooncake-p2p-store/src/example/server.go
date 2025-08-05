package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"
	"unsafe"

	"github.com/kvcache-ai/Mooncake/mooncake-p2p-store/src/p2pstore"
)

type RequestPayload struct {
	Command  string `json:"command"`
	FileName string `json:"filename"`
}

type AgentServer struct {
	agentID               int
	metaServer            string
	localServer           string
	device                string
	nicPriorityMatrixPath string
	p2pStore              *p2pstore.P2PStore
}

func NewAgentServer() *AgentServer {
	return &AgentServer{}
}

// length = 0: 从offset开始映射整个文件
func mmapFileSection(path string, offset uint64, length *uint64) ([]byte, error) {
	if offset < 0 || *length < 0 {
		return nil, fmt.Errorf("offset/length must be non-negative")
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file failed: %v\n", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file failed: %v\n", err)
	}
	fileSize := uint64(fi.Size())

	if offset >= fileSize {
		return nil, fmt.Errorf("offset is larger than file size")
	}

	if *length == 0 {
		*length = fileSize - offset
	} else if *length > fileSize-offset {
		return nil, fmt.Errorf("length is invalid")
	}

	addr, err := syscall.Mmap(int(f.Fd()), int64(offset), int(*length), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED) //, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("mmap file failed: %v\n", err)
	}
	fmt.Printf("mmap file success: addr %x size %d\n", uintptr(unsafe.Pointer(&addr[0])), length)
	return addr, nil
}

func (a *AgentServer) do_register(ctx context.Context, fileName string) error {
	store := a.p2pStore
	var fileSize uint64 = 0
	addr, err := mmapFileSection(fileName, 0, &fileSize)
	if err != nil {
		return fmt.Errorf("mmap file failed: %v\n", err)
	}

	fmt.Printf("Object registration: name %s base address %x file size %d MB\n",
		fileName,
		uintptr(unsafe.Pointer(&addr[0])),
		fileSize>>20)

	startTimestamp := time.Now()
	addrList := []uintptr{uintptr(unsafe.Pointer(&addr[0]))}
	sizeList := []uint64{uint64(fileSize)}

	const MAX_SHARD_SIZE uint64 = 64 * 1024 * 1024
	const MEMORY_LOCATION string = "cpu:0"

	err = store.Register(ctx, fileName, addrList, sizeList, MAX_SHARD_SIZE, MEMORY_LOCATION, true)
	if err != nil {
		return fmt.Errorf("registration failed: %v\n", err)
	}

	phaseOneTimestamp := time.Now()
	duration := phaseOneTimestamp.Sub(startTimestamp).Milliseconds()

	fmt.Printf("Object registration done: duration (ms) %d throughput (GB/s) %.2f\n",
		duration,
		float64(fileSize>>20)/float64(duration))

	checkpointInfoList, err := store.List(ctx, "/")
	if err != nil {
		return fmt.Errorf("List failed: %v\n", err)
	}

	fmt.Println(checkpointInfoList)
	return nil
}

func (a *AgentServer) do_get(ctx context.Context, fileName string) error {
	store := a.p2pStore
	fileMeta, err := store.Get(ctx, fileName)
	if err != nil {
		return fmt.Errorf("get metadata failed: %v\n", err)
	}
	var fileSize = fileMeta.Size

	dirPath := filepath.Dir(fileName)
	_, err = os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("dir not exist: ", dirPath)
		}
		return fmt.Errorf("stat dir failed: %v\n", err)
	}
	_, err = os.Stat(fileName)
	if err == nil {
		fmt.Fprintf(os.Stderr, "file already exist: %s, will be overwritten", fileName)
	}
	f, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("create file failed: %v\n", err)
	}
	if err := f.Truncate(int64(fileSize)); err != nil {
		return fmt.Errorf("truncate file failed: %v\n", err)
	}
	defer f.Close()

	addr, err := mmapFileSection(fileName, 0, &fileSize)
	if err != nil {
		return fmt.Errorf("mmap file failed: %v\n", err)
	}

	fmt.Println("Object retrieval started: name", fileName)
	startTimestamp := time.Now()
	addrList := []uintptr{uintptr(unsafe.Pointer(&addr[0]))}
	sizeList := []uint64{fileSize}
	err = store.GetReplica(ctx, fileName, addrList, sizeList)
	if err != nil {
		return fmt.Errorf("Object retrieval failed: %v\n", err)
	}

	phaseOneTimestamp := time.Now()
	duration := phaseOneTimestamp.Sub(startTimestamp).Milliseconds()

	fmt.Printf("Object retrieval done: duration (ms) %d throughput (GB/s) %.2f\n",
		duration,
		float64(fileSize>>20)/float64(duration))

	err = store.DeleteReplica(ctx, fileName)
	if err != nil {
		return fmt.Errorf("DeleteReplica failed: %v\n")
	}

	if err := syscall.Munmap(addr); err != nil {
		return fmt.Errorf("unmap failed: %v\n")
	}
	return nil
}

func (a *AgentServer) do_unregister(ctx context.Context, fileName string) error {
	store := a.p2pStore
	err := store.Unregister(ctx, fileName)
	if err != nil {
		return fmt.Errorf("unregister failed: %v\n", err)
	}
	fmt.Println("Object unregistration done name: ", fileName)
	return nil
}

func (a *AgentServer) ServeReq(w http.ResponseWriter, r *http.Request) {
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
	log.Println("(whorwe)GetReq: 0 | cmd: ", cmd, ", fileName: ", fileName)
	switch cmd {
	case "register":
		err := a.do_register(context.Background(), fileName)
		if err != nil {
			log.Println("register failed: ", err)
		}
	case "get":
		err := a.do_get(context.Background(), fileName)
		if err != nil {
			log.Println("get failed: ", err)
		}
	case "unregister":
		err := a.do_unregister(context.Background(), fileName)
		if err != nil {
			log.Println("unregister failed: ", err)
		}
	case "list":
		fileList, err := a.p2pStore.List(context.Background(), "/")
		if err != nil {
			log.Println("list failed: ", err)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fileList)
	default:
		log.Println("unknown command: ", cmd)
	}
}
