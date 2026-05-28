# AWS ElastiCache Redis Cluster

This folder contains a small CloudFormation stack for a development Redis OSS cluster-mode ElastiCache replication group.

ElastiCache is VPC-private. Run the coordinator/consumer/publisher containers from the same VPC, through VPN, or through a development NAT/bastion tunnel. A local laptop without VPC routing cannot connect directly to the ElastiCache endpoint.

## Deploy

Example values for the current default `ap-northeast-2` VPC:

```bash
aws cloudformation deploy \
  --stack-name redis-stream-coordinator-dev-redis \
  --template-file infra/aws/cloudformation/elasticache-redis-cluster.yaml \
  --parameter-overrides \
    ReplicationGroupId=redis-stream-coordinator-dev \
    VpcId=vpc-0caceb0e72c675ecb \
    SubnetIds=subnet-04db4c2719728a187,subnet-064c5def16502b53a,subnet-0712e102a80b3d6e1,subnet-0658df970cc21a099 \
    AllowedCidr=172.31.0.0/16 \
    CacheNodeType=cache.t4g.micro \
    ShardCount=3 \
    ReplicasPerShard=0
```

Get the configuration endpoint:

```bash
aws cloudformation describe-stacks \
  --stack-name redis-stream-coordinator-dev-redis \
  --query "Stacks[0].Outputs[?OutputKey=='ConfigurationEndpoint'].OutputValue" \
  --output text
```

Run the app stack against ElastiCache from an environment that can route to the VPC:

```bash
export AWS_REDIS_CLUSTER_NODES="<configuration-endpoint>:6379"
docker compose -f compose.aws-elasticache.yaml -p rsc-aws up -d --build
```

## Cleanup

```bash
docker compose -f compose.aws-elasticache.yaml -p rsc-aws down
aws cloudformation delete-stack --stack-name redis-stream-coordinator-dev-redis
```

## Public Development Redis Cluster

ElastiCache endpoints are VPC-private. If you need a Redis Cluster that local tools such as IntelliJ or `redis-cli -c` can reach directly over the internet, use the public EC2 development template instead. It runs three password-protected Redis nodes on one EC2 instance and restricts inbound Redis access to one client CIDR.

Do not use this for production.

```bash
aws cloudformation deploy \
  --stack-name redis-stream-coordinator-public-dev-redis \
  --template-file infra/aws/cloudformation/public-ec2-redis-cluster.yaml \
  --parameter-overrides \
    StackNamePrefix=redis-stream-coordinator-public-dev \
    VpcId=vpc-0caceb0e72c675ecb \
    SubnetId=subnet-064c5def16502b53a \
    AllowedClientCidr=<your-public-ip>/32 \
    RedisPassword=<strong-password>
```

Get the externally reachable cluster nodes:

```bash
aws cloudformation describe-stacks \
  --stack-name redis-stream-coordinator-public-dev-redis \
  --query "Stacks[0].Outputs[?OutputKey=='ClusterNodes'].OutputValue" \
  --output text
```

Run the local coordinator and sample pods against it:

```bash
scripts/with-aws-redis-secret.sh \
  docker compose -f compose.aws-public-redis.yaml -p rsc-aws-public up -d --build
```

Connect from host tools:

```bash
redis-cli -c -h <public-ip> -p 6379 -a "<strong-password>"
```

The compose file reads Redis connection values through Docker secrets sourced from the current shell environment. `scripts/with-aws-redis-secret.sh` loads those environment variables from AWS Secrets Manager secret `personal/beta` without writing a local `.env` file.
