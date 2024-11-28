#!/bin/bash

# 원본 디렉토리와 대상 디렉토리 설정
SOURCE_DIR="./data"
TARGET_DIR="./merged_data"

# 합칠 기준 날짜 설정 (YYYYMM)
START_MONTH="202411"

# 대상 디렉토리가 없으면 생성
if [ ! -d "$TARGET_DIR" ]; then
    mkdir -p "$TARGET_DIR"
fi


echo "Starting merge process..."

# 날짜 순회 (월별 역순)
CURRENT_MONTH="$START_MONTH"
while true; do
    echo "Processing files for month: $CURRENT_MONTH"
    OUTPUT_FILE="$TARGET_DIR/merged_${CURRENT_MONTH}.csv"
    
    # 해당 월에 해당하는 파일 찾기
    FILES=$(find "$SOURCE_DIR" -type f -name "article_*_${CURRENT_MONTH}*.csv" | sort -r)
    
    # 파일이 없으면 다음 달로 이동
    if [ -z "$FILES" ]; then
        break
    fi
    
    # 헤더 처리용 플래그
    HEADER_WRITTEN=false
    # 파일 내용 합치기
    for FILE in $FILES; do
        echo "Adding $FILE to $OUTPUT_FILE"
        if [ "$HEADER_WRITTEN" = false ]; then
            cat "$FILE" >> "$OUTPUT_FILE"
            HEADER_WRITTEN=true
        else
            # 헤더 제외하고 파일 병합
            tail -n +2 "$FILE" >> "$OUTPUT_FILE"
        fi
    done
    
    # 다음 월 계산 (YYYYMM 형식)
    YEAR=${CURRENT_MONTH:0:4}
    MONTH=${CURRENT_MONTH:4:2}
    MONTH=$((10#$MONTH - 1)) # 월을 숫자로 변환 후 감소
    if [ "$MONTH" -lt 1 ]; then
        MONTH=12
        YEAR=$((YEAR - 1))
    fi
    CURRENT_MONTH=$(printf "%04d%02d" $YEAR $MONTH)
done

echo "Merge process complete. Combined file saved to $OUTPUT_FILE."

echo "Proccess Done."