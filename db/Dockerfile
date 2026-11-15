FROM postgres:16-bookworm

# wal2json 逻辑解码输出插件（PGDG 官方仓库已随基础镜像配置好）
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-16-wal2json \
 && rm -rf /var/lib/apt/lists/*

COPY initdb/ /docker-entrypoint-initdb.d/
