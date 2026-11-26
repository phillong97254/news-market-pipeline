#!/bin/bash
# Đồng bộ code từ Kali lên Oracle rồi build + chạy bằng docker compose.
# Dùng:  ./deploy.sh                  (tất cả)
#        ./deploy.sh price-producer   (chỉ build lại 1 service)
set -euo pipefail
cd "$(dirname "$0")"
REMOTE_DIR=news-market-pipeline
rsync -az --delete --exclude-from=.rsync-exclude ./ "oracle:~/$REMOTE_DIR/"
ssh oracle "cd ~/$REMOTE_DIR && docker compose up -d --build $*"
ssh oracle "cd ~/$REMOTE_DIR && docker compose ps"
