#!/bin/zsh

ssh -i ~/.ssh/flink-cluster.pem -o StrictHostKeyChecking=no slave$1

