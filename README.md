awsfs
=====

View AWS infrastructure as a filesystem.

Requirements:
* FUSE
* AWS account and credentials

Timestamp support:
s3 bucket creation date = bucket directory creation date
s3 file mtime = file mtime
directory mtime = data reload time

