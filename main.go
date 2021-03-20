package main

import (
	"context"
	"flag"
	"log"
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
	}

	stable := fs.StableAttr{
		Mode: FILE_MODE,
		Ino:  0,
	}
	operations := &DBFS{}

	child := r.NewInode(ctx, operations, stable)

	// In case of concurrent lookup requests, it can happen that operations !=
	// child.Operations().
	return child, 0
}

var _ = (fs.NodeLookuper)((*DBFS)(nil))

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
