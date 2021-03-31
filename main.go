package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/replit/database-go"
)

const CACHE_LIFETIME = 5 * time.Second

const DIR_MODE = 0755  // drwxrwxr-x
const FILE_MODE = 0664 // -rw-rw-r--

type DBFS struct {
	fs.Inode

	keysCache   []string
	flushTicker *time.Ticker
}

func NewDBFS() DBFS {
	return DBFS{
		keysCache: make([]string, 0),
	}
}

func (d *DBFS) FlushCache() error {
	log.Println("Flushing key cache")

	// If the cache is being flushed now, reset the flush ticker so that it isn't
	//  needlessly flushed
	if d.flushTicker != nil {
		d.flushTicker.Reset(CACHE_LIFETIME)
	}

	keys, err := database.ListKeys("")
	if err != nil {
		return err
	}
	d.keysCache = keys
	return nil
}

func (d *DBFS) CacheFlushLoop(quit chan bool) {
	ticker := time.NewTicker(CACHE_LIFETIME)
	d.flushTicker = ticker
	for {
		select {
		case <-quit:
			ticker.Stop()
			return
		case <-ticker.C:
			err := d.FlushCache()
			if err != nil {
				panic(err)
			}
		}
	}
}

func (d *DBFS) KeyInCache(target string) bool {
	for _, k := range d.keysCache {
		if k == target {
			return true
		}
	}
	return false
}

func (r *DBFS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Readdir should be a way to invalidate the cache immediately and it should show the
	//  latest data, so flush cache immediately whenever this is called
	r.FlushCache()
	keys := r.keysCache

	entries := make([]fuse.DirEntry, len(keys))
	for i, k := range keys {
		entries[i] = fuse.DirEntry{
			Name: k,
			Ino:  0, // 0 = auto-generate the number
			Mode: FILE_MODE,
		}
	}

	return fs.NewListDirStream(entries), fs.OK
}

var _ = (fs.NodeReaddirer)((*DBFS)(nil))

func (r *DBFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("STATing key %s\n", name)
	// Ensure that the key exists
	if !r.KeyInCache(name) {
		return nil, syscall.ENOENT
	}

	stable := fs.StableAttr{
		Mode: FILE_MODE,
		Ino:  0,
	}
	operations := &KeyFile{key: name}

	child := r.NewInode(ctx, operations, stable)

	// In case of concurrent lookup requests, it can happen that operations !=
	// child.Operations().
	return child, 0
}

var _ = (fs.NodeLookuper)((*DBFS)(nil))

// bytesFileHandle is a file handle that has it's contents stored in memory
type bytesFileHandle struct {
	content []byte
}

// bytesFileHandle allows reads
var _ = (fs.FileReader)((*bytesFileHandle)(nil))

func (fh *bytesFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	end := off + int64(len(dest))
	if end > int64(len(fh.content)) {
		end = int64(len(fh.content))
	}

	// We could copy to the `dest` buffer, but since we have a
	// []byte already, return that.
	return fuse.ReadResultData(fh.content[off:end]), 0
}

type KeyFile struct {
	fs.Inode

	key     string
	mu      sync.Mutex
	content []byte
}

func (f *KeyFile) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// disallow writes (for now)
	if fuseFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}

	// Read key
	log.Printf("Open()ing database key %s\n", f.key)
	val, err := database.Get(f.key)
	if err != nil {
		if err == database.ErrNotFound {
			return nil, 0, syscall.ENOENT
		} else {
			log.Printf("Error reading database key %s: %s", f.key, err)
			return nil, 0, syscall.EAGAIN
		}
	}

	fh = &bytesFileHandle{content: []byte(val)}

	// Return FOPEN_DIRECT_IO so content is not cached.
	return fh, fuse.FOPEN_DIRECT_IO, syscall.F_OK
}

var _ = (fs.NodeOpener)((*KeyFile)(nil))

func main() {
	debug := flag.Bool("debug", false, "print debug data")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  hello MOUNTPOINT")
	}
	mountpoint := flag.Arg(0)

	dbfs := NewDBFS()

	// CacheFlushLoop waits for CACHE_LIFETIME before the first flush, so do it now to
	//  ensure that cache is fresh and we catch errors (such as missing DB url) early
	err := dbfs.FlushCache()
	if err != nil {
		log.Fatalf("Error populating keys cache: %s\n", err)
	}
	cacheFlushQuit := make(chan bool)
	go dbfs.CacheFlushLoop(cacheFlushQuit)

	opts := &fs.Options{}
	opts.Debug = *debug

	if *debug {
		log.Printf("Mounting on %s\n", mountpoint)
	}
	server, err := fs.Mount(mountpoint, &dbfs, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Wait()
	cacheFlushQuit <- true
}
