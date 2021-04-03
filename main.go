package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/replit/database-go"
)

const CACHE_LIFETIME = 5 * time.Second

type Cache struct {
	mu sync.Mutex
	// A cache of the current list of keys in the database.
	// It is re-populated with fresh data every time READDIR is called on the filessystem
	//  and on a loop every CACHE_LIFETIME (re-populating will delay the auto-refresh).
	// The cache is used to respond to STAT requests.
	keys        []string
	flushTicker *time.Ticker
}

func NewCache() *Cache {
	return &Cache{
		keys: make([]string, 0),
	}
}

func (d *Cache) ResetFlushTicker() {
	if d.flushTicker != nil {
		d.flushTicker.Reset(CACHE_LIFETIME)
	}
}

func (d *Cache) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	log.Println("Flushing key cache")

	// If the cache is being flushed now, reset the flush ticker so that it isn't
	//  needlessly flushed
	d.ResetFlushTicker()

	escapedKeys, err := database.ListKeys("")
	if err != nil {
		return err
	}
	// URL Decode keys
	// When the library starts doing this this code will no longer be needed
	keys := make([]string, len(escapedKeys))
	for i, k := range escapedKeys {
		unescapedKey, err := url.PathUnescape(k)
		if err != nil {
			panic(err)
		}
		keys[i] = unescapedKey
	}
	log.Printf("Got keys:\n%s\n", strings.Join(keys, "\n"))

	d.keys = keys
	return nil
}

func (d *Cache) CacheFlushLoop(quit chan bool) {
	ticker := time.NewTicker(CACHE_LIFETIME)
	d.flushTicker = ticker
	for {
		select {
		case <-quit:
			ticker.Stop()
			return
		case <-ticker.C:
			err := d.Flush()
			if err != nil {
				panic(err)
			}
		}
	}
}

func (d *Cache) KeyInCache(target string) bool {
	for _, k := range d.keys {
		if k == target {
			return true
		}
	}
	return false
}

type KeyDir struct {
	fs.Inode

	cache *Cache
	// path is guaranteed to be either "" or end with "/"
	// This is so that the root KeyDir's path can be "" and the corresponding database
	//  key for a given KeyFile is always f.parent.path + f.key, making subdir keys
	//  automatically separated with a slash
	path string
}

func NewKeyDir(cache *Cache, path string) *KeyDir {
	return &KeyDir{cache: cache, path: path}
}

func RemoveEmpty(in []string) []string {
	out := make([]string, 0)
	for _, i := range in {
		if i != "" {
			out = append(out, i)
		}
	}
	return out
}

// A file tree is an in-memory representation of files and directories.
type FileTree struct {
	files map[string]bool
	dirs  map[string]*FileTree
}

func NewFileTree() *FileTree {
	return &FileTree{files: make(map[string]bool), dirs: make(map[string]*FileTree)}
}

func (t *FileTree) GetDir(p string) *FileTree {
	d, exists := t.dirs[p]
	if exists {
		return d
	}
	n := NewFileTree()
	t.dirs[p] = n
	return n
}

func (t *FileTree) Add(path []string) {
	if len(path) == 0 {
		return
	}

	curr := t
	isDir := path[len(path)-1] == ""
	cleanPath := RemoveEmpty(path)
	if len(cleanPath) == 0 {
		return
	}

	last := len(cleanPath) - 1
	for _, p := range cleanPath[:last] {
		curr = curr.GetDir(p)
	}

	if isDir {
		curr.dirs[cleanPath[last]] = NewFileTree()
	} else {
		curr.files[cleanPath[last]] = true
	}
}

func (t *FileTree) AddString(path string) {
	t.Add(strings.Split(path, "/"))
}

func (t *FileTree) Show(indent int) {
	for p, d := range t.dirs {
		fmt.Printf("%s%s/\n", strings.Repeat(" ", indent), p)
		d.Show(indent + 2)
	}
	for f := range t.files {
		fmt.Printf("%s%s\n", strings.Repeat(" ", indent), f)
	}
}

func (t *FileTree) ToDirEntries(ctx context.Context, r *KeyDir) []fuse.DirEntry {
	numDirs := len(t.dirs)
	out := make([]fuse.DirEntry, numDirs+len(t.files))
	i := 0
	for dirPath := range t.dirs {
		out[i] = fuse.DirEntry{
			Name: dirPath,
			Ino:  0,
			Mode: fuse.S_IFDIR,
		}
		i++
	}
	for filePath := range t.files {
		out[i] = fuse.DirEntry{
			Name: filePath,
			Ino:  0,
			Mode: fuse.S_IFREG,
		}
		i++
	}
	return out
}

var _ = (fs.NodeReaddirer)((*KeyDir)(nil))

func (r *KeyDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Readdir should be a way to invalidate the cache immediately and it should show the
	//  latest data, so flush cache immediately whenever this is called
	r.cache.Flush()
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	allKeys := r.cache.keys
	// Filter keys to be just the ones in this directory
	var keys []string
	if r.path == "" {
		keys = allKeys
	} else {
		keys = make([]string, 0)
		// safe to append "/" because we already checked that r.path != ""
		prefix := r.path
		prefixLen := len(prefix)
		for _, k := range allKeys {
			if strings.HasPrefix(k, prefix) && len(k) > prefixLen {
				keys = append(keys, k[prefixLen:])
			}
		}
	}
	log.Printf("keys in '%s': \n%s\n", r.path, strings.Join(keys, "\n"))

	tree := NewFileTree()
	for _, k := range keys {
		tree.AddString(k)
	}
	log.Println("tree:")
	tree.Show(0)

	return fs.NewListDirStream(tree.ToDirEntries(ctx, r)), fs.OK
}

var _ = (fs.NodeUnlinker)((*KeyDir)(nil))

func (r *KeyDir) Unlink(ctx context.Context, name string) syscall.Errno {
	// No need to check for existence
	// See above warnings about urlencoding
	err := database.Delete(url.PathEscape(r.Key(name)))
	if err != nil {
		panic(err)
	}
	r.cache.Flush()
	return syscall.F_OK
}

var _ = (fs.NodeRmdirer)((*KeyDir)(nil))

func (r *KeyDir) Rmdir(ctx context.Context, name string) syscall.Errno {
	// Flush to make sure we are deleting ALL subitems
	r.cache.Flush()
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()

	prefix := r.Key(name) + "/"
	for _, k := range r.cache.keys {
		if strings.HasPrefix(k, prefix) {
			log.Printf("Deleting key %s", k)
			err := database.Delete(url.PathEscape(k))
			if err != nil {
				panic(err)
			}
		}
	}
	r.cache.mu.Unlock()
	r.cache.Flush()
	r.cache.mu.Lock() // so that defer does not cause unintended behavior
	return syscall.F_OK
}

type KeyFile struct {
	fs.Inode

	parent *KeyDir
	// Key holds the full database key that this file corresponds to
	key string
}

func NewKeyFile(parent *KeyDir, key string) KeyFile {
	return KeyFile{parent: parent, key: key}
}

func (r *KeyDir) NewKeyDirInode(ctx context.Context, name string) *fs.Inode {
	stable := fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  0,
	}
	operations := NewKeyDir(r.cache, name+"/")

	child := r.NewInode(ctx, operations, stable)

	return child
}

func (r *KeyDir) NewKeyFileInode(ctx context.Context, key string) *fs.Inode {
	stable := fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  0,
	}
	operations := NewKeyFile(r, key)

	child := r.NewInode(ctx, &operations, stable)

	// In case of concurrent lookup requests, it can happen that operations !=
	// child.Operations().
	return child
}

func (r *KeyDir) DirExists(key string) bool {
	// A single key a/ means an empty directory.
	dirKey := key + "/"
	if r.cache.KeyInCache(dirKey) {
		return true
	}
	// Check for contained files
	// a/ is valid if there are any keys that start with a/ (files/directories inside a/)
	for _, k := range r.cache.keys {
		if strings.HasPrefix(k, dirKey) {
			return true
		}
	}
	return false
}

func (r *KeyDir) Key(name string) string {
	return r.path + name
}

var _ = (fs.NodeLookuper)((*KeyDir)(nil))

func (r *KeyDir) Lookup(
	ctx context.Context,
	name string,
	out *fuse.EntryOut,
) (*fs.Inode, syscall.Errno) {
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	key := r.Key(name)
	log.Printf("STATing key %s\n", key)
	// Ensure that the key exists
	if r.cache.KeyInCache(key) {
		return r.NewKeyFileInode(ctx, key), syscall.F_OK
	}
	// Check if it is a directory
	if r.DirExists(key) {
		return r.NewKeyDirInode(ctx, key), syscall.F_OK
	}

	return nil, syscall.ENOENT
}

var _ = (fs.NodeMkdirer)((*KeyDir)(nil))

func (r *KeyDir) Mkdir(
	ctx context.Context,
	name string,
	mode uint32,
	out *fuse.EntryOut,
) (*fs.Inode, syscall.Errno) {
	key := r.Key(name) + "/"
	log.Printf("KeyDir %s: Mkdir %s with mode %d => key %s\n", r.path, name, mode, key)
	// temporary HACK: client does not URL encode keys so do it ourself
	// remove this once the library starts encoding so we don't double encode
	err := database.Set(url.PathEscape(key), "")
	if err != nil {
		panic(err)
	}
	r.cache.Flush()
	return r.NewKeyDirInode(ctx, key), syscall.F_OK
}

var _ = (fs.NodeAccesser)((*KeyFile)(nil))

func (r *KeyFile) Access(ctx context.Context, mask uint32) syscall.Errno {
	// Anyone can access this filesystem (ignore permission checks).
	// This squelches the confirm prompt from rm when trying to remove a key because the
	//  prompt is caused by a lack of write permissions
	return syscall.F_OK
}

// bytesFileHandle is a file handle that has it's contents stored in memory
type bytesFileHandle struct {
	content []byte
}

// bytesFileHandle allows reads
var _ = (fs.FileReader)((*bytesFileHandle)(nil))

func (fh *bytesFileHandle) Read(
	ctx context.Context, dest []byte, off int64,
) (fuse.ReadResult, syscall.Errno) {
	end := off + int64(len(dest))
	if end > int64(len(fh.content)) {
		end = int64(len(fh.content))
	}

	// We could copy to the `dest` buffer, but since we have a
	// []byte already, return that.
	return fuse.ReadResultData(fh.content[off:end]), 0
}

func (f *KeyFile) OpenRead(ctx context.Context) (
	fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno,
) {
	// Read key
	key := f.key
	log.Printf("Opening database key %s for reading\n", key)
	// see other warnings about urlencoding
	val, err := database.Get(url.PathEscape(key))
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

	parent  *KeyDir
	key     string
	mu      sync.Mutex
	content []byte
}

func NewKeyWriter(parent *KeyDir, key string) *KeyWriter {
	return &KeyWriter{parent: parent, key: key}
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

func (f *KeyWriter) Write(
	ctx context.Context, data []byte, off int64,
) (written uint32, errno syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	sz := int64(len(data))
	key := f.key
	log.Printf("Storing %d bytes in memory for key %s", sz, key)
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

	key := f.key
	log.Printf("Flushing %d bytes to key %s\n", len(f.content), key)
	// Actually write the content to the database
	err := database.Set(url.PathEscape(key), string(f.content))
	if err != nil {
		panic(err)
	}
	// Re-populate the cache so that future STATs have this key
	f.parent.cache.Flush()

	return syscall.F_OK
}

func SetOwner(out *fuse.AttrOut) {
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}
	uid, err := strconv.ParseUint(currentUser.Uid, 10, 32)
	// this will panic on windows but whatever
	if err != nil {
		panic(err)
	}
	gid, err := strconv.ParseUint(currentUser.Gid, 10, 32)
	if err != nil {
		panic(err)
	}
	log.Printf("Uid %d, Gid %d\n", uid, gid)
	// conversion is safe because 32 was passed to ParseUint
	out.Owner.Uid = uint32(uid)
	out.Owner.Gid = uint32(gid)
}

// Implement GetAttr to provide owner
var _ = (fs.NodeGetattrer)((*KeyFile)(nil))

func (bn *KeyFile) Getattr(
	ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut,
) syscall.Errno {
	SetOwner(out)
	return 0
}

// Implement GetAttr to provide owner
var _ = (fs.NodeGetattrer)((*KeyDir)(nil))

func (bn *KeyDir) Getattr(
	ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut,
) syscall.Errno {
	SetOwner(out)
	return 0
}

// Implement Setattr to support truncation
var _ = (fs.NodeSetattrer)((*KeyFile)(nil))

func (f *KeyFile) Setattr(
	ctx context.Context,
	fh fs.FileHandle,
	in *fuse.SetAttrIn,
	out *fuse.AttrOut,
) syscall.Errno {
	return 0
}

// Implement Setattr to make some applications happy
var _ = (fs.NodeSetattrer)((*KeyDir)(nil))

func (f *KeyDir) Setattr(
	ctx context.Context,
	fh fs.FileHandle,
	in *fuse.SetAttrIn,
	out *fuse.AttrOut,
) syscall.Errno {
	return 0
}

func (f *KeyFile) OpenWrite(ctx context.Context) (
	fh fs.FileHandle,
	fuseFlags uint32,
	errno syscall.Errno,
) {
	log.Printf("Opening key %s for writing\n", f.key)
	return NewKeyWriter(f.parent, f.key), fuse.FOPEN_NONSEEKABLE, syscall.F_OK
}

var _ = (fs.NodeCreater)((*KeyDir)(nil))

func (r *KeyDir) Create(
	ctx context.Context,
	name string,
	flags uint32,
	mode uint32,
	out *fuse.EntryOut,
) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	child := r.NewKeyFileInode(ctx, name)
	writer := NewKeyWriter(r, name)
	log.Printf("Creating key %s\n", name)
	return child, writer, fuse.FOPEN_NONSEEKABLE | fuse.FOPEN_DIRECT_IO, syscall.F_OK
}

var _ = (fs.NodeOpener)((*KeyFile)(nil))

func (f *KeyFile) Open(ctx context.Context, flags uint32) (
	fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno,
) {
	log.Printf("Open flags: %d\n", flags)
	if flags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
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

	cache := NewCache()
	// CacheFlushLoop waits for CACHE_LIFETIME before the first flush, so do it now to
	//  ensure that cache is fresh and we catch errors (such as missing DB url) early
	err := cache.Flush()
	if err != nil {
		log.Fatalf("Error populating keys cache: %s\n", err)
	}
	cacheFlushQuit := make(chan bool)
	go cache.CacheFlushLoop(cacheFlushQuit)

	root := NewKeyDir(cache, "")

	opts := &fs.Options{}
	opts.Debug = *debug

	if *debug {
		log.Printf("Mounting on %s\n", mountpoint)
	}
	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Wait()
	cacheFlushQuit <- true
}
