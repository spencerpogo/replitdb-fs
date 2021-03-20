package main

import (
	"context"
	"flag"
	"log"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type DBFS struct {
	fs.Inode
}

func (r *DBFS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// default directory permissions: drwxrwxr-x
	out.Mode = 0755
	return 0
}

var _ = (fs.NodeGetattrer)((*DBFS)(nil))

func (r *DBFS) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := make([]fuse.DirEntry, 1)
	entries[0] = fuse.DirEntry{
		Name: "abc",
		Ino:  0,
		Mode: 0664,
	}
	return fs.NewListDirStream(entries), fs.OK
}

var _ = (fs.NodeReaddirer)((*DBFS)(nil))

func (r *DBFS) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name != "abc" {
		return nil, syscall.ENOENT
	}

	stable := fs.StableAttr{
		Mode: 0664,
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
