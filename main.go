package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/replit/database-go"
)

const DIR_MODE = 0755  // drwxrwxr-x
const FILE_MODE = 0664 // -rw-rw-r--

type DBFS struct {
	fs.Inode
}

type KeyFile struct {
	fs.Inode

	key     string
	mu      sync.Mutex
	content []byte
}

func (r *DBFS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// List everything in the database
	keys, err := database.ListKeys("")
	if err != nil {
		log.Printf("Error while listing database keys: %s\n", err)
		return nil, syscall.EAGAIN
	}

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
	// Ensure that the key exists
	_, err := database.Get(name)
	if err == database.ErrNotFound {
		return nil, syscall.ENOENT
	} else if err != nil {
		log.Printf("Error STATing database key %s: %s", name, err)
		return nil, syscall.EAGAIN
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

	dbfs := &DBFS{}

	opts := &fs.Options{}
	opts.Debug = *debug

	if *debug {
		log.Printf("Mounting on %s\n", mountpoint)
	}
	server, err := fs.Mount(mountpoint, dbfs, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Wait()
}
