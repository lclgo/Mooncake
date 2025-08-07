package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	Command   string `json:"command"`
	FileName  string `json:"filename"`
	LocalName string `json:"localname"`
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
	blockNum     int
	blockSize    uint64
	fileSize     uint64
	fileAddr     uintptr
	registerName string
	localName    string
}

type MapInfo struct {
	addrList []uintptr
	sizeList []uint64
}

var fileMappings = make(map[string]*MapInfo)

const blockNum = 2

func updateMap(registerName string, addrList []uintptr, sizeList []uint64) {
	fileMappings[registerName] = &MapInfo{
		addrList: addrList,
		sizeList: sizeList,
	}
}

func deleteMap(registerName string) error {
	delete(fileMappings, registerName)
	return nil
}

func getMap(registerName string) *MapInfo {
	if info, ok := fileMappings[registerName]; ok {
		return info
	} else {
		return nil
	}
}

func NewAgentServer() *AgentServer {
	return &AgentServer{}
}

func NewBlockInfo(fileName string, fileSize uint64) *BlockInfo {
	alignMB := uint64(1)<<uint64(20) - uint64(1)
	blockSize := (fileSize/uint64(blockNum) + alignMB) & ^alignMB
	return &BlockInfo{
		blockNum:     blockNum,
		blockSize:    blockSize,
		fileSize:     fileSize,
		fileAddr:     0,
		registerName: fileName,
	}
}

// length = 0: 从offset开始映射整个文件
func mmapFileSection(path string, offset uint64, length *uint64) ([]byte, error) {
	if offset < 0 || *length < 0 {
		return nil, fmt.Errorf("offset/length must be non-negative")
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file failed: %v", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file failed: %v", err)
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

	addr, err := syscall.Mmap(int(f.Fd()), int64(offset), int(*length), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap file failed: %v", err)
	}
	fmt.Printf("mmap file success: addr %x size %d\n", uintptr(unsafe.Pointer(&addr[0])), *length)
	return addr, nil
}

func do_register(ctx context.Context, store *p2pstore.P2PStore, fileName string, addrList []uintptr, sizeList []uint64) error {
	fmt.Printf("Object registration: name %s base address %x file size %d\n",
		fileName,
		addrList[0],
		sizeList[0])

	startTimestamp := time.Now()
	const MAX_SHARD_SIZE uint64 = 128 * 1024 * 1024
	const MEMORY_LOCATION string = "cpu:0"

	err := store.Register(ctx, fileName, addrList, sizeList, MAX_SHARD_SIZE, MEMORY_LOCATION, true)
	if err != nil {
		return fmt.Errorf("registration failed: %v", err)
	}
	updateMap(fileName, addrList, sizeList)

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
	var addr []byte
	// if blockAddr == 0, means this file is not mapped
	if block.fileAddr == 0 {
		addr, err := mmapFileSection(block.localName, offset, &size)
		if err != nil {
			return fmt.Errorf("mmap file failed: %v", err)
		}
		blockAddr = uintptr(unsafe.Pointer(&addr[0]))
	} else {
		// shouldn't go to this branch, we now always create a
		// new map area for block.
		blockAddr = block.fileAddr + uintptr(offset)
	}

	blockName := fmt.Sprintf("%s:%d:%d", block.registerName, id, block.blockNum)
	addrList := []uintptr{blockAddr}
	sizeList := []uint64{size}
	err := do_register(ctx, a.p2pStore, blockName, addrList, sizeList)
	// NOTE: block.fileAddr is always 0, because we now always create
	// a new map area for block
	if err != nil && block.fileAddr == 0 {
		if syscall.Munmap(addr); err != nil {
			return fmt.Errorf("munmap file failed: %v", err)
		}
	}
	return err
}

func (a *AgentServer) register(ctx context.Context, fileName string) error {
	store := a.p2pStore
	var fileSize uint64 = 0
	addr, err := mmapFileSection(fileName, 0, &fileSize)
	if err != nil {
		return fmt.Errorf("mmap file failed: %v", err)
	}

	fileAddr := uintptr(unsafe.Pointer(&addr[0]))

	if err := do_register(ctx, store, fileName,
		[]uintptr{fileAddr},
		[]uint64{fileSize}); err != nil {
		if syscall.Munmap(addr); err != nil {
			return fmt.Errorf("munmap file failed: %v", err)
		}
		return err
	}

	if a.agentID == 0 && !strings.Contains(fileName, ":") {
		blockInfo := NewBlockInfo(fileName, fileSize)
		blockInfo.localName = fileName
		// blockInfo.fileAddr = fileAddr
		if err := a.do_register_block(ctx, blockInfo, a.agentID); err != nil {
			return err
		}
	}

	// checkpointInfoList, err := store.List(ctx, "/")
	// if err != nil {
	// 	return fmt.Errorf("List failed: %v", err)
	// }
	// fmt.Println(checkpointInfoList)

	return nil
}

func prepareFile(fileName string, fileSize uint64) (*os.File, error) {
	dirPath := filepath.Dir(fileName)
	_, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("dir not exist: ", dirPath)
		}
		return nil, fmt.Errorf("stat dir failed: %v", err)
	}
	_, err = os.Stat(fileName)
	if err == nil {
		fmt.Fprintf(os.Stderr, "file already exist: %s, will be overwritten\n", fileName)
	}
	f, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("create file failed: %v", err)
	}
	if err := f.Truncate(int64(fileSize)); err != nil {
		return nil, fmt.Errorf("truncate file failed: %v", err)
	}
	return f, nil
}

func copyFromLocal(registerName string, fileSize uint64, dstSlice []byte) error {
	mapInfo := getMap(registerName)
	if mapInfo.sizeList[0] != fileSize {
		return fmt.Errorf("file size mismatch: %d vs %d", mapInfo.sizeList[0], fileSize)
	}
	copyChunk := uint64(512 << 20) // 512M
	srcSlice := unsafe.Slice((*byte)(unsafe.Pointer(mapInfo.addrList[0])), fileSize)
	fmt.Fprintf(os.Stdout, "Copy payload from local, src: %x, dst: %x\n",
		mapInfo.addrList[0],
		uintptr(unsafe.Pointer(&dstSlice[0])))
	for i := uint64(0); i < fileSize; i += copyChunk {
		copySize := copyChunk
		if i+copyChunk > fileSize {
			copySize = fileSize - i
		}
		copy(dstSlice[i:i+copySize], srcSlice[i:i+copySize])
		// fmt.Fprintf(os.Stdout, "copy chunk: %d, fileSize: %d, copySize: %dM\n", i, fileSize, copySize>>20)
	}
	if err := syscall.Munmap(dstSlice); err != nil {
		return fmt.Errorf("unmap failed: %v", err)
	}
	return nil
}

func getReplicaToMem(ctx context.Context, store *p2pstore.P2PStore, registerName string, addr []byte, size uint64) error {
	addrList := []uintptr{uintptr(unsafe.Pointer(&addr[0]))}
	sizeList := []uint64{size}
	err := store.GetReplica(ctx, registerName, addrList, sizeList)
	if err != nil && err != p2pstore.ErrPayloadOpened {
		return fmt.Errorf("Object retrieval failed: %v", err)
	}

	if err == p2pstore.ErrPayloadOpened {
		copyFromLocal(registerName, size, addr)
	} else {
		updateMap(registerName, addrList, sizeList)
	}
	return nil
}

func (a *AgentServer) doGetFullFile(ctx context.Context, registerName string, localName string) error {
	store := a.p2pStore
	fileMeta, err := store.Get(ctx, registerName)
	if err != nil {
		return fmt.Errorf("get metadata failed: %v", err)
	}
	var fileSize = fileMeta.Size
	f, err := prepareFile(localName, fileSize)
	if err != nil {
		return fmt.Errorf("prepare file failed: %v", err)
	}
	defer f.Close()

	addr, err := mmapFileSection(localName, 0, &fileSize)
	if err != nil {
		return fmt.Errorf("mmap file failed: %v", err)
	}

	fmt.Fprintf(os.Stdout, "Object retrieval started: %s -> %s\n", registerName, localName)
	getReplicaToMem(ctx, store, registerName, addr, fileSize)

	// 这里不要DeleteReplica，不然node1 register bar，node2 get bar + register bar:1:2后
	// node1 get bar:1:2会失败。
	// 怀疑大致的原因是：DeleteReplica会将fileName映射的[addr, addr+fileSize)标记为无效，当我们执行
	// Munmap(addr)后，马上再执行Mmap()映射同一个文件，内核分配的addr大概率是一致的，还是落在[addr, addr+fileSize)
	// node1尝试获取node2注册的bar:1:2时，RDMA标记为无效的内存因为不可知的原因还未刷新，导致传输失败，重新get bar:1:2
	// 就能成功了。
	// 验证猜想：执行DeleteReplica后不Munmap()的情况下Mmap，get bar:1:2会成功。
	// err = store.DeleteReplica(ctx, fileName)
	// if err != nil {
	// 	return fmt.Errorf("DeleteReplica failed: %v\n")
	// }

	// if err := syscall.Munmap(addr); err != nil {
	// 	return fmt.Errorf("unmap failed: %v\n")
	// }

	if a.agentID > 0 && a.agentID < blockNum && !strings.Contains(registerName, ":") {
		blockInfo := NewBlockInfo(registerName, fileSize)
		blockInfo.localName = localName
		// blockInfo.fileAddr = uintptr(unsafe.Pointer(&addr[0]))
		if err := a.do_register_block(ctx, blockInfo, a.agentID); err != nil {
			return err
		}
	}
	return nil
}

func (a *AgentServer) getFile(ctx context.Context, registerName string, localName string) error {
	startTimestamp := time.Now()
	store := a.p2pStore
	fileMeta, err := store.Get(ctx, registerName)
	if err != nil {
		return fmt.Errorf("Get file meta failed: %v", err)
	}
	var fileSize = fileMeta.Size
	defer func() {
		phaseOneTimestamp := time.Now()
		duration := phaseOneTimestamp.Sub(startTimestamp).Milliseconds()

		fmt.Printf("Object retrieval done: duration (ms) %d throughput (GB/s) %.2f\n",
			duration,
			float64(fileSize>>20)/float64(duration))
	}()

	for i := 0; i < blockNum; i++ {
		_, err := store.Get(ctx, fmt.Sprintf("%s:%d:%d", registerName, i, blockNum))
		if err != nil {
			return a.doGetFullFile(ctx, registerName, localName)
		}
	}

	fmt.Fprintf(os.Stdout, "Object retrieval started (block mode): %s -> %s\n", registerName, localName)
	prepareFile(localName, fileSize)
	block := NewBlockInfo(registerName, fileSize)
	for i := uint64(0); i < blockNum; i++ {
		offset := i * block.blockSize
		size := block.blockSize
		if offset+size > block.fileSize {
			size = block.fileSize - offset
		}
		addr, err := mmapFileSection(localName, offset, &size)
		if err != nil {
			return fmt.Errorf("mmap file failed: %v", err)
		}
		blockRegisterName := fmt.Sprintf("%s:%d:%d", registerName, i, blockNum)
		getReplicaToMem(ctx, store, blockRegisterName, addr, size)
	}

	return nil
}

func (a *AgentServer) unregister(ctx context.Context, fileName string) error {
	store := a.p2pStore
	err := store.Unregister(ctx, fileName)
	if err != nil {
		return fmt.Errorf("unregister failed: %v", err)
	}
	mapInfo := getMap(fileName)
	if mapInfo == nil {
		return fmt.Errorf("cannot find mapInfo: %s", fileName)
	}
	unmapAddr := unsafe.Slice((*byte)(unsafe.Pointer(mapInfo.addrList[0])), mapInfo.sizeList[0])
	if err := syscall.Munmap(unmapAddr); err != nil {
		return fmt.Errorf("unmap failed: %v\n", err)
	}
	deleteMap(fileName)
	fmt.Println("Object unregistration done name: ", fileName)
	return nil
}

func (a *AgentServer) deleteFile(ctx context.Context, fileName string) error {
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
	localName := fileName
	if payload.LocalName != "" {
		localName = payload.LocalName
	}
	fmt.Fprintf(os.Stdout, "Got Request cmd: %s, fileName: %s\n", cmd, fileName)
	switch cmd {
	case "register":
		err := a.register(context.Background(), fileName)
		if err != nil {
			err_msg := fmt.Sprintf("register failed: %s", err)
			fmt.Fprintf(os.Stderr, err_msg+"\n")
			http.Error(w, err_msg, http.StatusInternalServerError)
		}
	case "get":
		err := a.getFile(context.Background(), fileName, localName)
		if err != nil {
			err_msg := fmt.Sprintf("get failed: %s", err)
			fmt.Fprintf(os.Stderr, err_msg+"\n")
			http.Error(w, err_msg, http.StatusInternalServerError)
		}
	case "unregister":
		err := a.unregister(context.Background(), fileName)
		if err != nil {
			err_msg := fmt.Sprintf("unregister failed: %s", err)
			fmt.Fprintf(os.Stderr, err_msg+"\n")
			http.Error(w, err_msg, http.StatusInternalServerError)
		}
	case "list":
		fileList, err := a.p2pStore.List(context.Background(), "/")
		if err != nil {
			err_msg := fmt.Sprintf("list failed: %s", err)
			fmt.Fprintf(os.Stderr, err_msg+"\n")
			http.Error(w, err_msg, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fileList)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
	}
}
