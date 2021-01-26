#!/usr/bin/env python3
# -*- coding:utf-8 -*- 
# author：yuanlang 
# creat_time: 2020/8/20 下午1:57
# file: redis_cluster_helper.py

import sys
from rediscluster import RedisCluster


def redis_cluster():
    startup_nodes = [
        {'host': 'rd8095ad.redis.db.cloud.papub', 'port': 11385}
    ]
    try:
        print("RedisCluster 启动中")
        conn = RedisCluster(startup_nodes=startup_nodes,
                            # 有密码要加上密码哦
                            skip_full_coverage_check=True,
                            decode_responses=True, password='WFJHNqSarWyVi635')
        print(conn.ping())
        return conn
    except Exception as e:
        print("connect error ", str(e))
        sys.exit(1)
