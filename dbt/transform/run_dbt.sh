#!/bin/bash

# Kiểm tra xem đã truyền vào ngày chưa
if [ -z "$1" ]; then
  echo "Cách dùng: $0 <ngày, định dạng YYYY-MM-DD> [full_refresh: 1 hoặc 0]"
  exit 1
fi

# Mặc định không full-refresh nếu không truyền vào
FULL_REFRESH=${2:-0}

# Ghép thêm giờ mặc định vào ngày
START_DATE="$1 00:00:00"

# Xây dựng câu lệnh dbt
if [ "$FULL_REFRESH" -eq 1 ]; then
  dbt run --full-refresh --vars "{\"batch_start\": \"$START_DATE\"}"
else
  dbt run --vars "{\"batch_start\": \"$START_DATE\"}"
fi
