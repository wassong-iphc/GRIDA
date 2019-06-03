# GRIDA

GRIDA means Grid Data Management Agent.

## Server

Grida is mainly a server, than can receive requests to interact with a
file catalog.  It is for example able to upload, download, or list
files.  It supports Dirac file catalog, and LFC as a legacy.

Grida is essentially used by the Vip platform.  The following
describes how to install and configure Grida for a Vip server.  The
version used are `v6r21p1` for Dirac, and `2.0.1` for Grida.

### Dirac installation

Grida for Vip uses the Dirac File Catalog (DFC), and the Dirac
commands to execute the transfers.  So it needs a recent version of
Dirac to be installed.  Here is how Dirac is installed for Vip.  You
may have to change the path to the proxy file.

```shell
su - vip
cd /home/vip
mkdir dirac
cd dirac
wget -O dirac-install --no-check-certificate -np https://raw.github.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py
chmod +x dirac-install
./dirac-install -V gridfr -r v6r21p1 -e COMDIRAC
source bashrc
export X509_USER_PROXY=<PATH TO PROXY FILE USED BY VIP>
dirac-configure defaults-gridfr.cfg
```

Here is the official installation guide for the Dirac client: https://github.com/DIRACGrid/DIRAC/wiki/ClientInstallation

### Starting the server as a service

The Grida server should run as a service on the same machine as the
Vip server.  This is because they share some disk space.  Grida
directly reads or write the file to transfer from disk.  The Grida
client sends the path of the file to handle to the Grida server.

Here is the content of the file `/etc/systemd/system/grida.service`:
```
[Unit]
Description=Grida server
After=network.target
[Service]
User=vip
Type=simple
WorkingDirectory=/home/vip/grida
Environment=JAVA_HOME=/etc/alternatives/jre_1.8.0
ExecStart=/etc/alternatives/jre_1.8.0/bin/java -jar /home/vip/grida/grida-server-2.0.1.jar
[Install]
WantedBy=multi-user.target
```

### Server configuration

The configuration for the server, adapted to Vip, is:
```
agent.port = 9006
agent.retrycount = 5
agent.min.available.diskspace = 0.02
lfc.host = lfc-biomed.in2p3.fr
preferredSEsList = SBG-disk,NIKHEF-disk,CPPM-disk
vo = biomed
bdii.host = cclcgtopbdii02.in2p3.fr
bdii.port = 2170
cache.list.max.entries = 30
cache.list.max.hours = 12
cache.files.max.size = 100.0
cache.files.path = /home/vip/grida/cache
pool.max.download = 10
pool.max.upload = 10
pool.max.delete = 5
pool.max.replication = 5
pool.max.history = 15
commands.type = dirac
dirac.bashrc = /home/vip/dirac/bashrc
```

The accepted values for `commands.type` are `lcg` and `dirac`.  If the
value is `dirac`, then the entry `dirac.bashrc` must be set to the
path of the `bashrc` of the dirac installation.  The dirac
installation must include `COMDIRAC`, as its commands are used in this
case.


## Client

The client is a library than can be embeded into a Java program.
It allows to interact with the server.

### Client command line

The client also has a command line, that can be used with the command:
```shell
java -jar grida-client-2.0.1.jar
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
