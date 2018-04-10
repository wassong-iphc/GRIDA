# GRIDA

GRIDA means Grid Data Management Agent.

## Server

Grida is mainly a server, than can receive requests to interact with a
file catalog.  It is for example able to upload, download, or list
files from a LCG or Dirac file catalog.

### Server configuration

The default configuration for the server is:
```
agent.port = 9006
agent.retrycount = 5
agent.min.available.diskspace = 0.1
lfc.host = lfc-biomed.in2p3.fr
vo = biomed
bdii.host = cclcgtopbdii02.in2p3.fr
bdii.port = 2170
cache.list.max.entries = 30
cache.list.max.hours = 12
cache.files.max.size = 100.0
cache.files.path = .cache
pool.max.download = 10
pool.max.upload = 10
pool.max.delete = 5
pool.max.replication = 5
pool.max.history = 120
commands.type = lcg
dirac.bashrc = needed_if_commands.type_is_dirac
```

To the above list, must be added 2 entries which are empty by default
(thus not shown):
- the preferred SE list, named `lfc.preferredSEsList`.
- the failover servers list, named `failover.servers`.

The accepted values for `commands.type` are `lcg` and `dirac`.  If the
value is `dirac`, then the entry `dirac.bashrc` must be set to the
path of the `bashrc` of the dirac installation.


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
