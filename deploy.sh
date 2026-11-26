#!/bin/bash
# Đồng bộ code từ Kali lên Oracle rồi build + chạy bằng docker compose.
# Dùng:  ./deploy.sh                  (tất cả)
#        ./deploy.sh price-producer   (chỉ build lại 1 service)
set -euo pipefail
cd "$(dirname "$0")"
REMOTE_DIR=news-market-pipeline
# --inplace: ghi đè lên file cũ (giữ inode). docker-compose mount từng file cấu hình ClickHouse vào container;
# rsync mặc định tạo file mới rồi đổi tên, khi đó container vẫn thấy bản cũ cho tới khi tạo lại container.
rsync -az --inplace --delete --exclude-from=.rsync-exclude ./ "oracle:~/$REMOTE_DIR/"
ssh oracle "cd ~/$REMOTE_DIR && docker compose up -d --build $*"
ssh oracle "cd ~/$REMOTE_DIR && docker compose ps"
