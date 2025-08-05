package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

type BlockInfo struct {
	blockNum  int
	blockSize uint64
	fileSize  uint64
	fileAddr  uintptr
	fileName  string
}

func NewAgentServer() *AgentServer {
	return &AgentServer{}
}

func NewBlockInfo(fileName string, fileSize uint64) *BlockInfo {
	blockNum := 2
	alignMB := uint64(1)<<uint64(20) - uint64(1)
	blockSize := (fileSize/uint64(blockNum) + alignMB) & ^alignMB
	return &BlockInfo{
		blockNum:  blockNum,
		blockSize: blockSize,
		fileSize:  fileSize,
		fileAddr:  0,
		fileName:  fileName,
	}
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
	fmt.Printf("mmap file success: addr %x size %d\n", uintptr(unsafe.Pointer(&addr[0])), *length)
	return addr, nil
}

func do_register(ctx context.Context, store *p2pstore.P2PStore, fileName string, addrList []uintptr, sizeList []uint64) error {
	fmt.Printf("Object registration: name %s base address %x file size %d MB\n",
		fileName,
		addrList[0],
		sizeList[0]>>20)

	startTimestamp := time.Now()
	const MAX_SHARD_SIZE uint64 = 64 * 1024 * 1024
	const MEMORY_LOCATION string = "cpu:0"

	err := store.Register(ctx, fileName, addrList, sizeList, MAX_SHARD_SIZE, MEMORY_LOCATION, true)
	if err != nil {
		return fmt.Errorf("registration failed: %v\n", err)
	}

	phaseOneTimestamp := time.Now()
	duration := phaseOneTimestamp.Sub(startTimestamp).Milliseconds()

	fmt.Printf("Object registration done: duration (ms) %d throughput (GB/s) %.2f\n",
		duration,
		float64(sizeList[0]>>20)/float64(duration))

	return nil
}

func (a *AgentServer) do_register_block(ctx context.Context, block *BlockInfo, id int) error {
	if id >= block.blockNum {
		return fmt.Errorf("block id is out of range")
	}

	offset := block.blockSize * uint64(id)
	size := block.blockSize
	if offset+size > block.fileSize {
		size = block.fileSize - offset
	}

	var blockAddr uintptr
	// if blockAddr == 0, means this file is not mapped
	if block.fileAddr == 0 {
		addr, err := mmapFileSection(block.fileName, offset, &size)
		if err != nil {
			return fmt.Errorf("mmap file failed: %v\n", err)
		}
		blockAddr = uintptr(unsafe.Pointer(&addr[0]))
	} else {
		blockAddr = block.fileAddr + uintptr(offset)
	}

	blockName := fmt.Sprintf("%s:%d:%d", block.fileName, id, block.blockNum)
	addrList := []uintptr{blockAddr}
	sizeList := []uint64{size}
	return do_register(ctx, a.p2pStore, blockName, addrList, sizeList)
}

func (a *AgentServer) register(ctx context.Context, fileName string) error {
	store := a.p2pStore
	var fileSize uint64 = 0
	addr, err := mmapFileSection(fileName, 0, &fileSize)
	if err != nil {
		return fmt.Errorf("mmap file failed: %v\n", err)
	}

	fileAddr := uintptr(unsafe.Pointer(&addr[0]))

	if err := do_register(ctx, store, fileName,
		[]uintptr{fileAddr},
		[]uint64{fileSize}); err != nil {
		return err
	}

	if !strings.Contains(fileName, ":") {
		blockInfo := NewBlockInfo(fileName, fileSize)
		blockInfo.fileAddr = fileAddr
		if err := a.do_register_block(ctx, blockInfo, 0); err != nil {
			return err
		}
	}

	checkpointInfoList, err := store.List(ctx, "/")
	if err != nil {
		return fmt.Errorf("List failed: %v\n", err)
	}

	fmt.Println(checkpointInfoList)
	return nil
}

func (a *AgentServer) getFile(ctx context.Context, fileName string) error {
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
		fmt.Fprintf(os.Stderr, "file already exist: %s, will be overwritten\n", fileName)
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

	// 这里必须要DeleteReplica，不然node1 register bar bar:0:2，node2 get bar, register bar:1:2
	// 再node1 get bar:1:2会失败，原因未知。
	// err = store.DeleteReplica(ctx, fileName)
	// if err != nil {
	// 	return fmt.Errorf("DeleteReplica failed: %v\n")
	// }

	if err := syscall.Munmap(addr); err != nil {
		return fmt.Errorf("unmap failed: %v\n")
	}

	if strings.Contains(fileName, ":") {
		return nil
	}

	blockInfo := NewBlockInfo(fileName, fileSize)
	if err = a.do_register_block(ctx, blockInfo, 1); err != nil {
		return fmt.Errorf("register block failed in getFile: %v\n", err)
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
	log.Println("(whorwe)GetReq: 0 | cmd:", cmd, ", fileName:", fileName)
	switch cmd {
	case "register":
		err := a.register(context.Background(), fileName)
		if err != nil {
			log.Println("register failed: ", err)
		}
	case "get":
		err := a.getFile(context.Background(), fileName)
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
