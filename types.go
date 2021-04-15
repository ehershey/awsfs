package main

import (
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/hanwen/go-fuse/v2/fs"
	"time"
)

type FileHandle struct {
}

type AwsRoot struct {
	fs.Inode
}

type rootSubdir struct {
	fs.Inode
	Name     string
	s3       s3info
	ec2      ec2info
	LoadTime time.Time
}

type bucketDir struct {
	fs.Inode
	Name        string
	Bucket      s3types.Bucket
	objects     map[string]s3types.Object
	subprefixes map[string]bool
	populated   bool
	LoadTime    time.Time
}

type instanceDir struct {
	fs.Inode
	Name      string
	Instance  ec2types.Instance
	populated bool
	LoadTime  time.Time
}

type s3object struct {
	fs.Inode
	Name   string
	Bucket s3types.Bucket
	Object s3types.Object
}

type s3info struct {
	Buckets map[string]s3types.Bucket
	Ctime   time.Time
	Mtime   time.Time
}

type ec2info struct {
	Instances map[string]ec2types.Instance
	Ctime     time.Time
	Mtime     time.Time
}

type Ec2AttributeNode struct {
	Ec2InstanceSubnode
	Value string
}
type Ec2VolumesNode struct {
	Ec2InstanceSubnode
	populated bool
}

type Ec2InstanceSubnode struct {
	fs.Inode
	Instance ec2types.Instance
}

// Ensure we implement interfaces
var _ = (fs.NodeLookuper)((*rootSubdir)(nil))
var _ = (fs.NodeReaddirer)((*rootSubdir)(nil))
var _ = (fs.NodeLookuper)((*instanceDir)(nil))
var _ = (fs.NodeReaddirer)((*instanceDir)(nil))
var _ = (fs.NodeLookuper)((*Ec2VolumesNode)(nil))
var _ = (fs.NodeReaddirer)((*Ec2VolumesNode)(nil))
var _ = (fs.NodeLookuper)((*bucketDir)(nil))
var _ = (fs.NodeReaddirer)((*bucketDir)(nil))
var _ = (fs.NodeGetattrer)((*rootSubdir)(nil))
var _ = (fs.NodeGetattrer)((*s3object)(nil))
var _ = (fs.NodeGetattrer)((*Ec2AttributeNode)(nil))
var _ = (fs.NodeOpener)((*Ec2AttributeNode)(nil))
var _ = (fs.NodeReader)((*Ec2AttributeNode)(nil))
var _ = (fs.NodeOpener)((*s3object)(nil))
var _ = (fs.NodeReader)((*s3object)(nil))
var _ = (fs.NodeGetattrer)((*bucketDir)(nil))
var _ = (fs.NodeGetattrer)((*instanceDir)(nil))
var _ = (fs.NodeGetattrer)((*AwsRoot)(nil))
var _ = (fs.NodeGetattrer)((*rootSubdir)(nil))
var _ = (fs.NodeOnAdder)((*AwsRoot)(nil))
