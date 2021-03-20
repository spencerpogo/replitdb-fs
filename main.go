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

type DBFS struct {
	fs.Inode

	keys []string
}

type KeyFile struct {
	fs.MemRegularFile
}

func (r *DBFS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// default directory permissions: drwxrwxr-x
	out.Mode = 0755
	return 0
}

func (r *DBFS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := make([]fuse.DirEntry, 1)
	entries[0] = fuse.DirEntry{
		Name: "abc",
		Ino:  0,
		Mode: 0664,
	}
	return fs.NewListDirStream(entries), fs.OK
}

var _ = (fs.NodeGetattrer)((*DBFS)(nil))
var _ = (fs.NodeReaddirer)((*DBFS)(nil))

func main() {
	debug := flag.Bool("debug", false, "print debug data")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  hello MOUNTPOINT")
	}
	mountpoint := flag.Arg(0)

	// Can fetch the keys every time the directory list happens, but prefetching keys is
	//  much faster. (Need to consider this further.)
	keys, err := database.ListKeys("")
	if err != nil {
		log.Fatalf("Error fetching intial database keys: %s\n", err)
	}

	dbfs := &DBFS{
		keys: keys,
	}

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
