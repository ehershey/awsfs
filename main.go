package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const big_default_timeout = "5m"

// TODO use config
const aws_profile = "ernie.org-ro"

// TODO use config
const aws_region = "us-east-1"

// TODO use config
const sts_timeout = 2 * time.Second

var cfg aws.Config

var startTime time.Time

func (n *bucketDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("bucketDir(%v).Readdir()", n.Name)

	path := n.Path(nil)

	parts := strings.Split(path, "/")
	prefix := strings.Join(parts[2:], "/")

	if len(prefix) > 0 {
		prefix += "/"
	}

	log.Printf("path: %v", path)
	log.Printf("prefix: %v", prefix)

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)

	delim := "/"
	// Get all results for ListObjectsV2 for a bucket
	newctx, cancelfunc := context.WithCancel(ctx)
	defer cancelfunc()
	output, err := client.ListObjectsV2(newctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(*n.Bucket.Name),
		Delimiter: &delim,
		Prefix:    &prefix,
	})
	if err != nil {
		fmt.Printf("got an error during ListObjects: %v", err)
		return nil, syscall.EIO
	}

	r := make([]fuse.DirEntry, 0, len(output.Contents)+len(output.CommonPrefixes))

	log.Println("first page results:")
	n.subprefixes = make(map[string]bool)
	n.objects = make(map[string]s3types.Object)
	for _, object := range output.Contents {
		objectname := strings.TrimPrefix(aws.ToString(object.Key), prefix)
		n.objects[objectname] = object
		log.Printf("key=%s size=%d", objectname, object.Size)
		d := fuse.DirEntry{
			Name: objectname,
			Ino:  0,
			Mode: fuse.S_IFREG,
		}
		r = append(r, d)
	}
	for _, common_prefix := range output.CommonPrefixes {
		subprefix := strings.TrimPrefix(strings.TrimSuffix(aws.ToString(common_prefix.Prefix), "/"), prefix)
		n.subprefixes[subprefix] = true
		log.Printf("subprefix=%s", subprefix)
		d := fuse.DirEntry{
			Name: subprefix,
			Ino:  0,
			Mode: fuse.S_IFDIR,
		}
		r = append(r, d)
	}
	n.populated = true
	dirstream := fs.NewListDirStream(r)
	log.Printf("created the thang\n")
	return dirstream, 0
}

func (n *rootSubdir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("rootSubdir(%v).Readdir()", n.Name)

	switch n.Name {
	case "ec2":
		return n.getEc2DirStream(ctx)
		// return getEmptyDirStream(ctx)
	case "s3":
		return n.getS3DirStream(ctx)
		// return getEmptyDirStream(ctx)
	default:
		return getEmptyDirStream(ctx)
	}
}

func (n *rootSubdir) getEc2DirStream(ctx context.Context) (fs.DirStream, syscall.Errno) {
	r := make([]fuse.DirEntry, 0, 10)
	//for i := 0; i < len(subdirs); i++ {
	//d := fuse.DirEntry{
	//Name: subdirs[i],
	//Ino:  0,
	//Mode: fuse.S_IFDIR,
	//}
	//r = append(r, d)
	//}
	dirstream := fs.NewListDirStream(r)
	log.Printf("created the thang\n")
	return dirstream, 0
}

func getEmptyDirStream(ctx context.Context) (fs.DirStream, syscall.Errno) {
	r := make([]fuse.DirEntry, 0, 10)
	//for i := 0; i < len(subdirs); i++ {
	//d := fuse.DirEntry{
	//Name: subdirs[i],
	//Ino:  0,
	//Mode: fuse.S_IFDIR,
	//}
	//r = append(r, d)
	//}
	dirstream := fs.NewListDirStream(r)
	log.Printf("created the thang\n")
	return dirstream, 0
}
func (n *rootSubdir) getS3DirStream(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)

	output, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("buckets")

	r := make([]fuse.DirEntry, 0, len(output.Buckets))
	n.s3 = s3info{make(map[string]s3types.Bucket), time.Now(), time.Now()}
	for _, bucket := range output.Buckets {
		n.s3.Buckets[*bucket.Name] = bucket
		log.Printf(*bucket.Name)
		log.Printf(bucket.CreationDate.Format("2006-01-02 15:04:05"))
		d := fuse.DirEntry{
			Name: *bucket.Name,
			Ino:  0,
			Mode: fuse.S_IFDIR,
		}
		r = append(r, d)
	}
	dirstream := fs.NewListDirStream(r)
	log.Printf("created the thang\n")
	return dirstream, 0
}

func (n *bucketDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("bucketDir(%v).Lookup(%v)\n", n.Name, name)

	log.Printf("n: %v", n)

	parentName, parentInode := n.Parent()
	log.Printf("parentName: %v\n", parentName)
	log.Printf("parentInode: %v\n", parentInode)
	// log.Printf("parents: %v\n", n.parents)

	if !n.populated {
		log.Println("Triggering prefix readdir from child lookup")
		_, errno := n.Readdir(ctx)
		if errno != 0 {
			log.Println("Returning errno from child lookup readdir")
			return nil, errno
		}
	}
	log.Printf("Triggered prefix readdir complete (objects: %d, subprefixes: %d)\n", len(n.objects), len(n.subprefixes))

	// if the entry is in objects[] it's an object and not a prefix
	//
	if info, prs := n.objects[name]; prs == true {
		ops := s3object{Name: name, Bucket: n.Bucket, Object: info}
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFREG})
		log.Println("Returning new file inode")
		return newinode, 0
	} else if n.subprefixes[name] {
		ops := bucketDir{Name: name, Bucket: n.Bucket, LoadTime: time.Now()}
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFDIR})
		log.Println("Returning new bucketdir inode")
		return newinode, 0
	} else {
		log.Println("Returning ENOENT")
		return nil, syscall.ENOENT
	}
}

func (n *rootSubdir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("rootSubdir(%v).Lookup(%v)\n", n.Name, name)

	if n.Name == "s3" && len(n.s3.Buckets) == 0 {
		_, errno := n.getS3DirStream(ctx)
		if errno != 0 {
			return nil, errno
		}
	}

	bucket, prs := n.s3.Buckets[name]
	if !prs {
		return nil, syscall.ENOENT
	}
	ops := bucketDir{Name: name, Bucket: bucket, LoadTime: time.Now()}
	newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFDIR})
	log.Printf("returning new bucketDir inode")
	return newinode, 0
}

func (r *AwsRoot) OnAdd(ctx context.Context) {
	log.Println("OnAdd()")

	ch := r.NewPersistentInode(
		ctx, &fs.MemRegularFile{
			Data: []byte("file.txt\n"),
			Attr: fuse.Attr{
				Mode:  0644,
				Mtime: uint64(time.Now().Unix()),
				Ctime: uint64(time.Now().Unix()),
				Atime: uint64(time.Now().Unix()),
			},
		}, fs.StableAttr{Ino: 0})

	ch2 := r.NewPersistentInode(
		ctx, &fs.MemRegularFile{
			Data: []byte("file2.txt\n"),
			Attr: fuse.Attr{
				Mode:  0755,
				Mtime: uint64(time.Now().Unix()),
				Ctime: uint64(time.Now().Unix()),
				Atime: uint64(time.Now().Unix()),
			},
		}, fs.StableAttr{Ino: 0})

	r.AddChild("file.txt", ch, false)
	r.AddChild("file2.txt", ch2, false)

	for _, subdir := range subdirs {
		ch3 := r.NewPersistentInode(
			ctx, &rootSubdir{fs.Inode{}, subdir, s3info{}, time.Now()},
			fs.StableAttr{Ino: 0,
				Mode: fuse.S_IFDIR,
			})
		r.AddChild(subdir, ch3, false)
	}
}

func (r *rootSubdir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	log.Printf("rootSubdir(%v).Getattr()", r.Name)
	out.Mode = 0755
	out.Mtime = uint64(r.LoadTime.Unix())
	out.Ctime = uint64(r.LoadTime.Unix())
	out.Atime = uint64(r.LoadTime.Unix())
	log.Printf("out.Mtime: %d", out.Mtime)
	return 0
}

func (o *s3object) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	log.Printf("s3object(%v).Getattr()", o.Name)

	out.Mode = 0755
	//mtime, err := time.Parse(time.RFC3339, o.Object.LastModified)
	//if err != nil {
	//log.Printf("Got error parsing mtime from s3: %v", err)
	//return syscall.EIO
	//}
	out.Mtime = uint64(o.Object.LastModified.Unix())
	out.Ctime = uint64(o.Object.LastModified.Unix())
	out.Atime = uint64(o.Object.LastModified.Unix())
	out.Size = uint64(o.Object.Size)
	log.Printf("out: %v", out)
	return 0
}

func (o *s3object) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Printf("s3object(%v).Open()\n", o.Name)
	fmt.Printf("flags: %d / %b\n", flags, flags)
	fmt.Printf("syscall.O_APPEND: %d / %b\n", syscall.O_APPEND, syscall.O_APPEND)
	fmt.Printf("syscall.O_CREAT: %d / %b\n", syscall.O_CREAT, syscall.O_CREAT)
	fmt.Printf("syscall.O_RDWR: %d / %b\n", syscall.O_RDWR, syscall.O_RDWR)
	fmt.Printf("flags & syscall.O_RDWR: %d / %b\n", flags&syscall.O_RDWR, flags&syscall.O_RDWR)

	// only support read = flags == 0
	if flags == 0 {
		fh := NewFileHandle()
		return fh, 0, 0
	}
	return nil, 0, syscall.EROFS
}

func NewFileHandle() fs.FileHandle {
	return FileHandle{}
}

func (o *s3object) Read(ctx context.Context, f fs.FileHandle, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	log.Printf("s3object(%v).Read()\n", o.Name)
	fmt.Printf("f: %d\n", f)
	fmt.Printf("len(dest): %v\n", len(dest))
	fmt.Printf("offset: %v\n", offset)

	client := s3.NewFromConfig(cfg)
	options := s3.GetObjectInput{Bucket: o.Bucket.Name, Key: o.Object.Key}

	if offset > o.Object.Size {
		log.Printf("offset greater than file size - I/O error")
		return nil, syscall.EIO
	}
	if offset != 0 {
		// return nil, syscall.ENOTSUP
		// request a range - to the end if that's everything, or at most enough to
		// fill dest, which may or may not actually get filled but the kernel might
		// make more offset reads
		//
		var last_byte_pos int
		if int(o.Object.Size) < len(dest)+int(offset)-1 {
			last_byte_pos = int(o.Object.Size)
		} else {
			last_byte_pos = len(dest) + int(offset) - 1
		}
		rangespec := fmt.Sprintf("bytes=%d-%d", offset, last_byte_pos)
		options.Range = &rangespec
		log.Printf("Requesting range: %v\n", options.Range)
	}

	output, err := client.GetObject(ctx, &options)
	if err != nil {
		log.Printf("Got error from GetObject: %v", err)
		return nil, syscall.EIO
	}
	bytes_read, err := io.ReadFull(output.Body, dest)
	log.Printf("Read %d bytes from s3\n", bytes_read)
	// if err == syscall.EOF
	if err != nil && err != io.EOF {
		log.Printf("Got error reading body from GetObject: %v\n", err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(dest), 0
}

func (r *bucketDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	log.Printf("bucketDir(%v).Getattr()", r.Name)
	out.Mode = 0755
	out.Mtime = uint64(r.LoadTime.Unix())
	out.Ctime = uint64(r.Bucket.CreationDate.Unix())
	out.Atime = uint64(r.Bucket.CreationDate.Unix())
	log.Printf("out: %v", out)
	return 0
}

func (r *AwsRoot) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	return 0
}

func main() {

	debug := flag.Bool("debug", false, "print debug data")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  awsfs [ --debug ] MOUNTPOINT")
	}
	// cache the heck out of everything we can
	bigd, err := time.ParseDuration(big_default_timeout)
	if err != nil {
		log.Fatal("Invalid timeout in code: ", err)
	}
	log.Printf("bigd: %v", bigd)

	if err = initaws(); err != nil {
		log.Fatal(err)
	}

	// opts := &fs.Options{}
	opts := &fs.Options{
		EntryTimeout: &bigd,
		//AttrTimeout:     &bigd, // seems to break initial mtime display
		NegativeTimeout: &bigd,
	}
	opts.Debug = *debug
	server, err := fs.Mount(flag.Arg(0), &AwsRoot{}, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	fmt.Printf("waiting\n")
	defer fmt.Printf("defered before waiting\n")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go waitforint(server, c)

	server.Wait()
	fmt.Printf("Done waiting\n")
}

func waitforint(server *fuse.Server, c chan os.Signal) {
	// Block until a signal is received.
	s := <-c
	if s == os.Interrupt {
		fmt.Println("Unmounting on SIGINT")
	} else {
		fmt.Println("Weird signal:", s)
	}
	server.Unmount()

	waitforint(server, c)
}

func initaws() (err error) {
	// Load the Shared AWS Configuration (~/.aws/config)
	// ctx := context.Background()
	ctx, cancelfunc := context.WithTimeout(context.Background(), sts_timeout)
	defer cancelfunc()
	cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(aws_region),
		config.WithSharedConfigProfile(aws_profile))

	if err != nil {
		return fmt.Errorf("Cannot load AWS configuration: %w", err)
	}
	client := sts.NewFromConfig(cfg)
	_, err = client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("Cannot authenticate to AWS: %w", err)
	}
	return
}
