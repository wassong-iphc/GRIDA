# GRIDA

GRIDA means Grid Data Management Agent.

## Server

Grida is mainly a server, than can receive requests to interact with a
file catalog.  It is for example able to upload, download, or list
files from a LCG or Dirac file catalog.

## Client

The client is a library than can be embeded into a Java program.
It allows to interact with the server.

### Client command line

The client also has a command line, that can be used with the command:
```shell
java -jar grida-client-1.4.1.jar
```

When this command is run without arguments, it prints the usage to
standard output:

```shell
usage: gridaClient [options] <command> <args>
Options:
 -h,--host <arg>    host of the server (default localhost)
 -p,--port <arg>    port of the server (default 9006)
 -r,--proxy <arg>   path of the user's proxy file (default
                    $X509_USER_PROXY)

<command> is one of the following (case insensitive):
 getFile <remoteFile> <localDir>
 getFolder <remoteDir> <localDir>
 list <dir> <1 if refresh, or else 0>
 getModDate <filename>
 upload <localFile> <remoteDir>
 uploadToSes <localFile> <remoteDir> <storageElement>
 replicate <remoteFile>
 delete <path to file or dir>
 createFolder <path>
 rename <oldPath> <newPath>
 exists <remotePath>
 setComment [lfn:]<path> <rev>
 listWithComment <dir> <1 if refresh, or else 0>
 cacheList
 cacheDelete <path>
 poolAdd <localFile> <remoteDir> <user>
 poolById <id>
 poolByUser <user>
 poolRemoveById <id>
 poolRemoveByUser <user>
 poolAll
 poolByDate <user> <limit> <startDate>
 zombieGet
 zombieDelete <surl>
```
