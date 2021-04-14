package main

import (
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

var subdirs = [...]string{"ec2", "s3", "iam"}

// Ensure we implement interfaces
var _ = (fs.NodeLookuper)((*rootSubdir)(nil))
var _ = (fs.NodeReaddirer)((*rootSubdir)(nil))
var _ = (fs.NodeLookuper)((*bucketDir)(nil))
var _ = (fs.NodeReaddirer)((*bucketDir)(nil))
var _ = (fs.NodeGetattrer)((*rootSubdir)(nil))
var _ = (fs.NodeGetattrer)((*s3object)(nil))
var _ = (fs.NodeOpener)((*s3object)(nil))
var _ = (fs.NodeReader)((*s3object)(nil))
var _ = (fs.NodeGetattrer)((*bucketDir)(nil))
var _ = (fs.NodeGetattrer)((*AwsRoot)(nil))
var _ = (fs.NodeGetattrer)((*rootSubdir)(nil))
var _ = (fs.NodeOnAdder)((*AwsRoot)(nil))
