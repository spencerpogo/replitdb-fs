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

type KeyFile struct {
	fs.MemRegularFile
}

func (r *DBFS) OnAdd(ctx context.Context) {
	ch := r.NewPersistentInode(
		ctx,
		&KeyFile{
			fs.MemRegularFile{
				Data: []byte("Hello world"),
				Attr: fuse.Attr{
					// default file permissions: -rw-rw-r--
					Mode: 0664,
				},
			},
		},
		fs.StableAttr{
			Mode: fuse.S_IFREG,
			Ino:  2,
		})
	r.AddChild("hello.txt", ch, false)
}

func (r *DBFS) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// default directory permissions: drwxrwxr-x
	out.Mode = 0755
	return 0
}

var _ = (fs.NodeGetattrer)((*DBFS)(nil))
var _ = (fs.NodeOnAdder)((*DBFS)(nil))

func main() {
	debug := flag.Bool("debug", false, "print debug data")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  hello MOUNTPOINT")
	}
	mountpoint := flag.Arg(0)

	opts := &fs.Options{}
	opts.Debug = *debug

	if *debug {
		log.Printf("Mounting on %s\n", mountpoint)
	}
	server, err := fs.Mount(mountpoint, &DBFS{}, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Wait()
}
