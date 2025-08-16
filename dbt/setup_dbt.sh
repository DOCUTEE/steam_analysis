#/bin/bash

service ssh start

# Giữ container sống (có thể thay bằng lệnh dbt run nếu muốn auto-run)
tail -f /dev/null