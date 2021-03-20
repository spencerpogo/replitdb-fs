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

func (r *DBFS) OnAdd(ctx context.Context) {
	for _, k := range r.keys {
		// for each key, add a regular file with the filename being the key.
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
				// Inode = 0 means autogenerate an inode number
				Ino: 0,
			})
		r.AddChild(k, ch, false)
	}
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
