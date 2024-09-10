#!/bin/bash

# 检查是否传入了目录参数
if [ $# -ne 1 ]; then
    echo "Usage: $0 <folder-path>"
    exit 1
fi

# 获取传入的目录
FOLDER=$1

# 检查目录是否存在
if [ ! -d "$FOLDER" ]; then
    echo "The directory $FOLDER does not exist."
    exit 1
fi

# 遍历目录中的所有 .csv 文件
for csv_file in "$FOLDER"/*.csv; do
    # 检查是否有 .csv 文件
    if [ -f "$csv_file" ]; then
        echo "Processing $csv_file ..."
        # 执行命令 ./dig-go，并传递当前的csv文件路径
        ./dig-go -dir "$csv_file"
        
        # 检查命令是否成功
        if [ $? -ne 0 ]; then
            echo "Error occurred while processing $csv_file"
        else
            echo "Successfully processed $csv_file"
        fi
    else
        echo "No CSV files found in $FOLDER"
        exit 1
    fi
done
