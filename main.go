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

	// A cache of the current list of keys in the database.
	// It is re-populated with fresh data every time READDIR is called on the filessystem
	//  and on a loop every CACHE_LIFETIME (re-populating will delay the auto-refresh).
	// The cache is used to respond to STAT requests.
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

var _ = (fs.NodeReaddirer)((*DBFS)(nil))

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

type KeyFile struct {
	fs.Inode

	fs      *DBFS
	key     string
	mu      sync.Mutex
	content []byte
}

func NewKeyFile(fs *DBFS, key string) KeyFile {
	return KeyFile{fs: fs, key: key}
}

func (r *DBFS) NewKeyInode(ctx context.Context, key string) *fs.Inode {
	stable := fs.StableAttr{
		Mode: FILE_MODE,
		Ino:  0,
	}
	operations := &KeyFile{key: key}

	child := r.NewInode(ctx, operations, stable)

	// In case of concurrent lookup requests, it can happen that operations !=
	// child.Operations().
	return child
}

func (r *DBFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("STATing key %s\n", name)
	// Ensure that the key exists
	if !r.KeyInCache(name) {
		return nil, syscall.ENOENT
	}

	return r.NewKeyInode(ctx, name), 0
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

func (f *KeyFile) OpenRead(ctx context.Context) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// Read key
	log.Printf("Opening database key %s for reading\n", f.key)
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

type KeyWriter struct {
	fs.Inode

	fs      *DBFS
	key     string
	mu      sync.Mutex
	content []byte
}

func NewKeyWriter(fs *DBFS, key string) *KeyWriter {
	return &KeyWriter{fs: fs, key: key}
}

func (bn *KeyWriter) Resize(sz uint64) {
	if sz > uint64(cap(bn.content)) {
		n := make([]byte, sz)
		copy(n, bn.content)
		bn.content = n
	} else {
		bn.content = bn.content[:sz]
	}
}

// KeyFile allows write
var _ = (fs.FileWriter)((*KeyWriter)(nil))

func (f *KeyWriter) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	sz := int64(len(data))
	log.Printf("Storing %d bytes in memory for key %s", sz, f.key)
	if off+sz > int64(len(f.content)) {
		f.Resize(uint64(off + sz))
	}
	copy(f.content[off:], data)
	return uint32(sz), syscall.F_OK
}

// KeyFile supports flushing
var _ = (fs.FileWriter)((*KeyWriter)(nil))

func (f *KeyWriter) Flush(ctx context.Context) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()

	log.Printf("Flushing %d bytes to key %s\n", len(f.content), f.key)
	// Actually write the content to the database
	err := database.Set(f.key, string(f.content))
	if err != nil {
		panic(err)
	}
	// Re-populate the cache so that future STATs have this key
	f.fs.FlushCache()

	return syscall.F_OK
}

// Implement GetAttr to provide size
var _ = (fs.NodeGetattrer)((*KeyWriter)(nil))

func (bn *KeyWriter) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	bn.mu.Lock()
	defer bn.mu.Unlock()
	bn.getattr(out)
	return 0
}

func (f *KeyWriter) getattr(out *fuse.AttrOut) {
	out.Size = uint64(len(f.content))
}

// Implement Setattr to support truncation
var _ = (fs.NodeSetattrer)((*KeyWriter)(nil))

func (f *KeyWriter) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()

	if sz, ok := in.GetSize(); ok {
		f.Resize(sz)
	}
	f.getattr(out)
	return 0
}

func (f *KeyFile) OpenWrite(ctx context.Context) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Printf("Opening key %s for writing\n", f.key)
	return NewKeyWriter(f.fs, f.key), fuse.FOPEN_NONSEEKABLE, syscall.F_OK
}

var _ = (fs.NodeCreater)((*DBFS)(nil))

func (r *DBFS) Create(
	ctx context.Context,
	name string,
	flags uint32,
	mode uint32,
	out *fuse.EntryOut,
) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	child := r.NewKeyInode(ctx, name)
	writer := NewKeyWriter(r, name)
	return child, writer, fuse.FOPEN_NONSEEKABLE | fuse.FOPEN_DIRECT_IO, syscall.F_OK
}

var _ = (fs.NodeOpener)((*KeyFile)(nil))

func (f *KeyFile) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Printf("Open flags: %d\n", fuseFlags)
	if fuseFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return f.OpenWrite(ctx)
	} else {
		return f.OpenRead(ctx)
	}
}

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
