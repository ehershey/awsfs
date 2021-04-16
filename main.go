package main

/*
 types:
 AwsRoot - the mounted filesystem root
 - rootSubdir - roughly 1:1 to aws service - ec2, s3, iam, etc
   - bucketDir - one for each bucket
     - bucketDir - artificial glue directory representing common prefix
     - s3object - s3 object
   - ec2Subdir - i for instances, v for volumes, vpc for vpc, snap for snapshots
	   - instanceDir
		   - Ec2AttributeNode - simple file containing attribute value
			 - Ec2VolumesNode - directory of attached volumes
			   - Ec2AttachedVolume
	   - volumeDir
	   - vpcDir
	   - snapshotDir
   - iamSubdir - users, groups, policies, roles
*/

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
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
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
const default_aws_region = "us-east-1"

// TODO use config
const sts_timeout = 2 * time.Second

var cfg aws.Config

var region_cfgs map[string]aws.Config

var startTime time.Time

var subdirs = [3]string{"ec2", "s3", "iam"}

func (r *ec2TagsDir) OnAdd(ctx context.Context) {
	log.Printf("ec2TagsDir().OnAdd()\n")

	for _, tag := range r.tags {

		key := tag.Key
		value := fmt.Sprintf("%s\n", *tag.Value)

		child := r.NewPersistentInode(ctx, &fs.MemRegularFile{
			Data: []byte(value),
			Attr: fuse.Attr{
				Mode:  0644,
				Mtime: uint64(time.Now().Unix()),
				Ctime: uint64(time.Now().Unix()),
				Atime: uint64(time.Now().Unix()),
			}}, fs.StableAttr{Ino: 0})
		r.AddChild(*key, child, false)
	}
	return
}

func (n *ec2TagsDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return getExistingDirStream(n.Children())
}
func (n *Ec2VolumesNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("Ec2VolumesNode(%v).Readdir()", n.Instance.InstanceId)

	r := make([]fuse.DirEntry, 0, len(n.Instance.BlockDeviceMappings))

	for _, object := range n.Instance.BlockDeviceMappings {
		volume_id := *object.Ebs.VolumeId
		d := fuse.DirEntry{
			Name: volume_id,
			Ino:  0,
			Mode: fuse.S_IFDIR,
		}
		r = append(r, d)
	}
	n.populated = true
	dirstream := fs.NewListDirStream(r)
	return dirstream, 0
}

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
	return dirstream, 0
}

func (n *volumeDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("volumeDir(%v).Readdir()", n.Name)

	r := make([]fuse.DirEntry, 0, 0)

	d := fuse.DirEntry{
		Name: "volume_type",
		Ino:  0,
		Mode: fuse.S_IFREG,
	}
	r = append(r, d)

	d2 := fuse.DirEntry{
		Name: "state",
		Ino:  0,
		Mode: fuse.S_IFREG,
	}

	r = append(r, d2)

	d3 := fuse.DirEntry{
		Name: "tags",
		Ino:  0,
		Mode: fuse.S_IFREG,
	}
	r = append(r, d3)

	n.populated = true
	dirstream := fs.NewListDirStream(r)
	return dirstream, 0
}

func (n *instanceDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("instanceDir(%v).Readdir()", n.Name)

	r := make([]fuse.DirEntry, 0, 0)

	d := fuse.DirEntry{
		Name: "instance_type",
		Ino:  0,
		Mode: fuse.S_IFREG,
	}
	r = append(r, d)

	d2 := fuse.DirEntry{
		Name: "state",
		Ino:  0,
		Mode: fuse.S_IFREG,
	}
	r = append(r, d2)

	vols := fuse.DirEntry{
		Name: "volumes",
		Ino:  0,
		Mode: fuse.S_IFDIR,
	}
	r = append(r, vols)
	n.populated = true
	dirstream := fs.NewListDirStream(r)
	return dirstream, 0
}

func (n *rootSubdir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("rootSubdir(%v).Readdir()", n.Name)

	// maybe change to type switch somehow?
	switch n.Name {
	case "ec2":
		return getExistingDirStream(n.Children())
	case "s3":
		return n.getS3DirStream(ctx)
	default:
		return getEmptyDirStream(ctx)
	}
}

func getExistingDirStream(kids map[string]*fs.Inode) (fs.DirStream, syscall.Errno) {
	//log.Printf("%v\n", n.ec2)
	r := make([]fuse.DirEntry, 0, len(kids))
	for name, child := range kids {
		child_direntry := fuse.DirEntry{
			Name: name,
			Ino:  child.StableAttr().Ino,
			Mode: fuse.S_IFDIR,
		}
		r = append(r, child_direntry)
	}
	return fs.NewListDirStream(r), 0
}

//type awsService interface {
//NewFromConfig(cfg)
//}
var regions = make([]string, 0, 0)

func getRegions(ctx context.Context) ([]string, error) {
	log.Printf("getRegions()\n")

	if len(regions) == 0 {
		client := ec2.NewFromConfig(cfg)

		output, err := client.DescribeRegions(ctx, &ec2.DescribeRegionsInput{})
		if err != nil {
			return nil, err
		}
		for _, region := range output.Regions {
			regions = append(regions, *region.RegionName)
		}
	}

	log.Printf("returning %d regions\n", len(regions))
	return regions, nil
}

func (n *ec2Subdir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("ec2Subdir(%v).Readdir()\n", n.Name)
	regions, err := getRegions(ctx)
	if err != nil {
		log.Println(err)
		return nil, syscall.EIO
	}

	if n.Name == "i" {
		if n.instances.populated {
			return getExistingDirStream(n.Children())
		}

		instances := make([]ec2types.Instance, 0, 0)

		for _, region := range regions {
			fmt.Printf("region: %+v\n", region)

			regionCfg, err := getRegionCfg(ctx, region)
			client := ec2.NewFromConfig(*regionCfg)

			InstanceOutput, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{})
			if err != nil {
				log.Println(err)
				return nil, syscall.EIO
			}

			log.Printf("instances\n")

			log.Printf("len(reservations): %d\n", len(InstanceOutput.Reservations))
			for _, res := range InstanceOutput.Reservations {
				for _, instance := range res.Instances {
					log.Printf("len(res.instances): %d\n", len(res.Instances))
					instances = append(instances, instance)
					log.Printf("len(instances): %d\n", len(instances))
				}
			}
		}

		r := make([]fuse.DirEntry, 0, len(instances))
		instancemap := make(map[string]ec2types.Instance)
		for _, instance := range instances {
			instancemap[*instance.InstanceId] = instance
			log.Printf(*instance.InstanceId)
			d := fuse.DirEntry{
				Name: *instance.InstanceId,
				Ino:  0,
				Mode: fuse.S_IFDIR,
			}
			r = append(r, d)
		}
		n.instances.Instances = instancemap
		n.instances.populated = true
		dirstream := fs.NewListDirStream(r)
		return dirstream, 0
	} else if n.Name == "v" {
		if n.volumes.populated {
			return getExistingDirStream(n.Children())
		}

		volumes := make([]ec2types.Volume, 0, 0)

		for _, region := range regions {
			fmt.Printf("region: %+v\n", region)

			regionCfg, err := getRegionCfg(ctx, region)
			client := ec2.NewFromConfig(*regionCfg)

			volumeOutput, err := client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{})
			if err != nil {
				log.Println(err)
				return nil, syscall.EIO
			}

			log.Printf("volumes\n")

			log.Printf("len(volumes): %d\n", len(volumeOutput.Volumes))
			for _, volume := range volumeOutput.Volumes {
				log.Printf("len(volumeOutput.volumes): %d\n", len(volumeOutput.Volumes))
				volumes = append(volumes, volume)
				log.Printf("len(volumes): %d\n", len(volumes))
			}
		}

		r := make([]fuse.DirEntry, 0, len(volumes))
		volumemap := make(map[string]ec2types.Volume)
		for _, volume := range volumes {
			volumemap[*volume.VolumeId] = volume
			log.Printf(*volume.VolumeId)
			d := fuse.DirEntry{
				Name: *volume.VolumeId,
				Ino:  0,
				Mode: fuse.S_IFDIR,
			}
			r = append(r, d)
		}
		n.volumes.Volumes = volumemap
		n.volumes.populated = true
		dirstream := fs.NewListDirStream(r)
		return dirstream, 0
	}
	return nil, syscall.ENOENT
}

func getEmptyDirStream(ctx context.Context) (fs.DirStream, syscall.Errno) {
	r := make([]fuse.DirEntry, 0, 10)
	dirstream := fs.NewListDirStream(r)
	return dirstream, 0
}

func (n *rootSubdir) getS3DirStream(ctx context.Context) (fs.DirStream, syscall.Errno) {
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
		d := fuse.DirEntry{
			Name: *bucket.Name,
			Ino:  0,
			Mode: fuse.S_IFDIR,
		}
		r = append(r, d)
	}
	dirstream := fs.NewListDirStream(r)
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
		ops := bucketDir{Name: name, Bucket: n.Bucket, loadTime: time.Now()}
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFDIR})
		log.Println("Returning new bucketdir inode")
		return newinode, 0
	} else {
		log.Println("Returning ENOENT")
		return nil, syscall.ENOENT
	}
}

func (n *instanceDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("instanceDir(%v).Lookup(%v)\n", n.Name, name)

	log.Printf("n: %v", n)

	if !n.populated {
		log.Println("Triggering instance readdir from child lookup")
		_, errno := n.Readdir(ctx)
		if errno != 0 {
			log.Println("Returning errno from child lookup readdir")
			return nil, errno
		}
	}
	log.Printf("Triggered instance readdir complete\n")

	if name == "instance_type" {
		ops := Ec2AttributeNode{Value: fmt.Sprintf("%v\n", n.Instance.InstanceType)}
		//ops.Resource = n.Instance
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFREG})
		log.Println("Returning new file inode")
		return newinode, 0
	} else if name == "state" {
		ops := Ec2AttributeNode{Value: fmt.Sprintf("%v\n", n.Instance.State.Name)}
		//ops.Instance = n.Instance
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFREG})
		log.Println("Returning new file inode")
		return newinode, 0
	} else if name == "tags" {
		ops := ec2TagsDir{}
		ops.tags = n.Instance.Tags
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFDIR})
		log.Println("Returning new ec2TagsDir inode")
		return newinode, 0
	} else if name == "volumes" {
		ops := Ec2VolumesNode{}
		ops.Instance = n.Instance
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFDIR})
		log.Println("Returning new Ec2VolumesNode inode")
		return newinode, 0
	} else {
		log.Println("Returning ENOENT")
		return nil, syscall.ENOENT
	}
}

func (n *volumeDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("volumeDir(%v).Lookup(%v)\n", n.Name, name)

	log.Printf("n: %v", n)

	if !n.populated {
		log.Println("Triggering volume readdir from child lookup")
		_, errno := n.Readdir(ctx)
		if errno != 0 {
			log.Println("Returning errno from child lookup readdir")
			return nil, errno
		}
	}
	log.Printf("Triggered volume readdir complete\n")

	if name == "volume_type" {
		ops := Ec2AttributeNode{Value: fmt.Sprintf("%v\n", n.Volume.VolumeType)}
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFREG})
		log.Println("Returning new file inode")
		return newinode, 0
	} else if name == "state" {
		ops := Ec2AttributeNode{Value: fmt.Sprintf("%v\n", n.Volume.State)}
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFREG})
		log.Println("Returning new file inode")
		return newinode, 0
	} else if name == "tags" {
		ops := ec2TagsDir{}
		ops.tags = n.Volume.Tags
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFDIR})
		log.Println("Returning new ec2TagsDir inode")
		return newinode, 0
	} else {
		log.Println("Returning ENOENT")
		return nil, syscall.ENOENT
	}
}

func (n *Ec2VolumesNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("Ec2VolumesNode(%v).Lookup(%v)\n", n.Instance.InstanceId, name)
	ops := Ec2AttributeNode{Value: "something"}
	newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFREG})
	log.Println("Returning new file inode")
	return newinode, 0
}
func (n *ec2TagsDir) Lookup(cts context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var newinode = n.Children()[name]
	return newinode, 0
}
func (n *rootSubdir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("rootSubdir(%v).Lookup(%v)\n", n.Name, name)

	if n.Name == "s3" {
		if len(n.s3.Buckets) == 0 {
			_, errno := n.getS3DirStream(ctx)
			if errno != 0 {
				return nil, errno
			}
		}

		bucket, prs := n.s3.Buckets[name]
		if !prs {
			return nil, syscall.ENOENT
		}
		ops := bucketDir{Name: name, Bucket: bucket, loadTime: time.Now()}
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFDIR})
		log.Printf("returning new bucketDir inode")
		return newinode, 0
	} else if n.Name == "ec2" {

		var newinode = n.Children()[name]
		return newinode, 0
	}
	return nil, syscall.ENOENT
}

func (r *rootSubdir) OnAdd(ctx context.Context) {
	log.Printf("rootSubdir(%v).OnAdd()", r.Name)

	ec2subdirs := []string{"i", "v", "vpc", "snapshot"}

	if r.Name == "ec2" {
		for _, subdir := range ec2subdirs {
			child := r.NewPersistentInode(
				ctx, &ec2Subdir{fs.Inode{}, subdir, instanceinfo{}, volumeinfo{}, vpcinfo{}, snapshotinfo{}, time.Now()},
				fs.StableAttr{Ino: 0,
					Mode: fuse.S_IFDIR,
				})
			r.AddChild(subdir, child, false)
		}
	}
	return
}

func (n *ec2Subdir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("ec2Subdir(%v).Lookup(%v)\n", n.Name, name)

	if n.Name == "i" {
		if !n.instances.populated {
			_, errno := n.Readdir(ctx)
			if errno != 0 {
				return nil, errno
			}
		}

		instance, prs := n.instances.Instances[name]
		if !prs {
			return nil, syscall.ENOENT
		}
		ops := instanceDir{Name: name, Instance: instance, loadTime: time.Now()}
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFDIR})
		log.Printf("returning new instanceDir inode")
		return newinode, 0
	} else if n.Name == "v" {
		if len(n.volumes.Volumes) == 0 {
			_, errno := n.Readdir(ctx)
			if errno != 0 {
				return nil, errno
			}
		}

		volume, prs := n.volumes.Volumes[name]
		if !prs {
			return nil, syscall.ENOENT
		}
		ops := volumeDir{Name: name, Volume: volume, loadTime: time.Now()}
		newinode := n.NewInode(ctx, &ops, fs.StableAttr{Mode: syscall.S_IFDIR})
		log.Printf("returning new volumeDir inode")
		return newinode, 0
	}

	return nil, syscall.ENOENT
}

func (r *AwsRoot) OnAdd(ctx context.Context) {
	log.Println("AwsRoot.OnAdd()")

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
		child := r.NewPersistentInode(
			ctx, &rootSubdir{fs.Inode{}, subdir, s3info{}, ec2info{}, time.Now()},
			fs.StableAttr{Ino: 0,
				Mode: fuse.S_IFDIR,
			})
		r.AddChild(subdir, child, false)
	}
}

func (r *volumeDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	return getBoringAttr(r, getLoadTime(r), out)
}

func (r *ec2TagsDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	return getBoringAttr(r, getLoadTime(r), out)
}

func (r *rootSubdir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	return getBoringAttr(r, getLoadTime(r), out)
}

func (o *s3object) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if res := getBoringAttr(o, *o.Object.LastModified, out); res != 0 {
		return res
	}
	out.Size = uint64(o.Object.Size)
	return 0
}

func (o *Ec2AttributeNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	//log.Printf("Ec2AttributeNode().Getattr()")

	out.Mode = 0755
	//mtime, err := time.Parse(time.RFC3339, o.Object.LastModified)
	//if err != nil {
	//log.Printf("Got error parsing mtime from s3: %v", err)
	//return syscall.EIO
	//}

	parentName, parentInode := o.Parent()
	log.Printf("parentName: %v\n", parentName)
	log.Printf("parentInode: %v\n", parentInode)

	out.Mtime = uint64(time.Now().Unix())
	out.Ctime = uint64(time.Now().Unix())
	out.Atime = uint64(time.Now().Unix())
	out.Size = uint64(len(o.Value))
	//log.Printf("out: %v", out)
	return 0
}

func (o *Ec2AttributeNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	log.Printf("Ec2AttributeNode().Open()\n")
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

func (o *Ec2AttributeNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	log.Printf("Ec2AttributeNode().Read()\n")
	fmt.Printf("f: %d\n", f)
	fmt.Printf("len(dest): %v\n", len(dest))
	fmt.Printf("offset: %v\n", offset)
	fmt.Printf("o.Value: %v\n", o.Value)

	copy(dest, o.Value)
	return fuse.ReadResultData(dest), 0
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
	return getBoringAttr(r, *r.Bucket.CreationDate, out)
}

func (r *instanceDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	return getBoringAttr(r, *r.Instance.LaunchTime, out)
}

type awsfsNode interface {
	LoadTime() time.Time
}

func (r *s3object) LoadTime() time.Time {
	return r.loadTime
}

func (r *ec2TagsDir) LoadTime() time.Time {
	return r.loadTime
}

func (r *volumeDir) LoadTime() time.Time {
	return r.loadTime
}

func (r *instanceDir) LoadTime() time.Time {
	return r.loadTime
}
func (r *rootSubdir) LoadTime() time.Time {
	return r.loadTime
}
func (r *bucketDir) LoadTime() time.Time {
	return r.loadTime
}
func getLoadTime(node awsfsNode) time.Time {
	return node.LoadTime()
}

func getBoringAttr(node awsfsNode, ctime time.Time, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	out.Mtime = uint64(node.LoadTime().Unix())
	out.Ctime = uint64(ctime.Unix())
	out.Atime = uint64(ctime.Unix())
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

// populate default region config object. Other region configs can be used and accessed
// by calling getRegionCfg(ctx, region)
func initaws() (err error) {

	// Load the Shared AWS Configuration (~/.aws/config)
	// ctx := context.Background()
	ctx, cancelfunc := context.WithTimeout(context.Background(), sts_timeout)
	defer cancelfunc()
	var default_region_cfg *aws.Config
	default_region_cfg, err = getRegionCfg(ctx, default_aws_region)

	cfg = *default_region_cfg

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

func getRegionCfg(ctx context.Context, region string) (*aws.Config, error) {
	if regionCfg, prs := region_cfgs[region]; prs {
		return &regionCfg, nil
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region),
		config.WithSharedConfigProfile(aws_profile))

	if err != nil {
		return nil, fmt.Errorf("Cannot load AWS configuration: %w", err)
	}
	return &cfg, nil
}
