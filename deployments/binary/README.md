# Install Milvus standalone through binary files

In order to quickly install and experience Milvus without docker or kubernetes, this document provides a tutorial for installing Milvus and dependencies, etcd and Minio, through binary files.

Before installing etcd and MinIO, you can refer to [docker-compose.yml](https://github.com/milvus-io/milvus/blob/master/deployments/docker/standalone/docker-compose.yml) to check the versions required by etcd and MinIO.

## Start etcd service

#### Refer: https://github.com/etcd-io/etcd/releases

```bash
wget https://github.com/etcd-io/etcd/releases/download/v3.5.0/etcd-v3.5.0-linux-amd64.tar.gz
tar zxvf etcd-v3.5.0-linux-amd64.tar.gz
cd etcd-v3.5.0-linux-amd64
./etcd -advertise-client-urls=http://127.0.0.1:2379 -listen-client-urls http://0.0.0.0:2379 --data-dir /etcd
```

## Start MinIO service

#### Refer: https://min.io/download#/linux

```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
./minio server /minio
```

## Start Milvus standalone service

### To start Milvus service, you need a Milvus binary file. Currently you can get the latest version of Milvus binary file through the Milvus docker image. (we will upload Milvus binary files in the future)

```bash
docker run -d --name milvus milvusdb/milvus:v2.0.0-rc6-20210910-020f109 /bin/bash
docker cp milvus:/milvus .
```

### Install Milvus dependencies

```bash
sudo apt-get install libopenblas-dev
sudo apt-get install libgomp1
sudo apt-get install libtbb2
```

### Start Milvus service

```bash
cd milvus
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PWD/lib
./bin/milvus run standalone
```
