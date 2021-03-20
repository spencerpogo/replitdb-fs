package main

import (
	"flag"
	"log"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type DBFS struct {
	fuse.RawFileSystem
}

func NewDBFS() *DBFS {
	return &DBFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
	}
}

func (fs *DBFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	log.Printf("GetAttr happened: %+v\n", input)
	return fuse.ENOSYS
}

func (fs *DBFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return fuse.ENOSYS
}

func main() {
	debug := flag.Bool("debug", false, "print debug data")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  hello MOUNTPOINT")
	}
	opts := &fuse.MountOptions{}
	opts.Debug = *debug

	fs := NewDBFS()
	mountpoint := flag.Arg(0)

	server, err := fuse.NewServer(fs, mountpoint, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Serve()
}
