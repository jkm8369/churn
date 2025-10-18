#!/usr/bin/env python3
"""
데이터베이스 초기화 스크립트
"""

import os
import sys
from sqlalchemy import create_engine, text
from database import DATABASE_URL, init_db, test_connection
from models import Base
import redis
import time

def wait_for_postgres(max_retries=30, delay=2):
    """PostgreSQL 연결 대기"""
    print("PostgreSQL 연결 대기 중...")
    
    for attempt in range(max_retries):
        try:
            if test_connection():
                print("✅ PostgreSQL 연결 성공!")
                return True
        except Exception as e:
            print(f"❌ 연결 시도 {attempt + 1}/{max_retries} 실패: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    
    print("❌ PostgreSQL 연결 실패!")
    return False

def wait_for_redis(max_retries=30, delay=2):
    """Redis 연결 대기"""
    print("Redis 연결 대기 중...")
    
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    for attempt in range(max_retries):
        try:
            r = redis.from_url(redis_url)
            r.ping()
            print("✅ Redis 연결 성공!")
            return True
        except Exception as e:
            print(f"❌ 연결 시도 {attempt + 1}/{max_retries} 실패: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    
    print("❌ Redis 연결 실패!")
    return False

def create_database_if_not_exists():
    """데이터베이스가 없으면 생성"""
    try:
        # 기본 postgres DB에 연결하여 churn_analysis DB 생성
        base_url = DATABASE_URL.rsplit('/', 1)[0] + '/postgres'
        engine = create_engine(base_url)
        
        with engine.connect() as conn:
            # 자동 커밋 모드로 전환
            conn.execute(text("COMMIT"))
            
            # 데이터베이스 존재 확인
            result = conn.execute(text(
                "SELECT 1 FROM pg_database WHERE datname = 'churn_analysis'"
            ))
            
            if not result.fetchone():
                print("churn_analysis 데이터베이스 생성 중...")
                conn.execute(text("CREATE DATABASE churn_analysis"))
                print("✅ 데이터베이스 생성 완료!")
            else:
                print("✅ 데이터베이스가 이미 존재합니다.")
                
    except Exception as e:
        print(f"⚠️ 데이터베이스 생성 건너뛰기: {e}")

def create_tables():
    """테이블 생성"""
    print("테이블 생성 중...")
    try:
        init_db()
        print("✅ 테이블 생성 완료!")
        return True
    except Exception as e:
        print(f"❌ 테이블 생성 실패: {e}")
        return False

def create_indexes():
    """추가 인덱스 생성"""
    print("인덱스 생성 중...")
    
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_events_user_month ON events (user_hash, DATE_TRUNC('month', created_at));",
        "CREATE INDEX IF NOT EXISTS idx_events_created_at_desc ON events (created_at DESC);",
        "CREATE INDEX IF NOT EXISTS idx_events_action_created_at ON events (action, created_at);",
        "CREATE INDEX IF NOT EXISTS idx_monthly_metrics_year_month ON monthly_metrics (year_month);",
        "CREATE INDEX IF NOT EXISTS idx_user_segments_composite ON user_segments (year_month, segment_type, segment_value);",
    ]
    
    try:
        from database import engine
        with engine.connect() as conn:
            for idx_sql in indexes:
                try:
                    conn.execute(text(idx_sql))
                    conn.commit()
                except Exception as e:
                    print(f"⚠️ 인덱스 생성 건너뛰기: {e}")
        
        print("✅ 인덱스 생성 완료!")
        return True
        
    except Exception as e:
        print(f"❌ 인덱스 생성 실패: {e}")
        return False

def insert_sample_data():
    """샘플 데이터 삽입"""
    print("샘플 데이터 삽입 중...")
    
    try:
        from database import SessionLocal
        from models import Event
        from datetime import datetime
        
        db = SessionLocal()
        
        # 기존 데이터 확인
        existing_count = db.query(Event).count()
        if existing_count > 0:
            print(f"✅ 이미 {existing_count}개의 이벤트가 존재합니다.")
            db.close()
            return True
        
        # 샘플 이벤트 생성
        sample_events = [
            Event(
                user_hash="sample_user_001",
                created_at=datetime(2025, 10, 1, 10, 0, 0),
                action="post",
                gender="M",
                age_band="30s",
                channel="web"
            ),
            Event(
                user_hash="sample_user_002",
                created_at=datetime(2025, 10, 2, 14, 30, 0),
                action="comment",
                gender="F",
                age_band="20s",
                channel="app"
            ),
            Event(
                user_hash="sample_user_003",
                created_at=datetime(2025, 10, 3, 9, 15, 0),
                action="post",
                gender="M",
                age_band="40s",
                channel="web"
            )
        ]
        
        db.bulk_save_objects(sample_events)
        db.commit()
        db.close()
        
        print("✅ 샘플 데이터 삽입 완료!")
        return True
        
    except Exception as e:
        print(f"❌ 샘플 데이터 삽입 실패: {e}")
        return False

def main():
    """메인 초기화 함수"""
    print("🚀 이탈자 분석 시스템 초기화 시작...")
    
    # 환경 확인
    print(f"DATABASE_URL: {DATABASE_URL}")
    print(f"ENVIRONMENT: {os.getenv('ENVIRONMENT', 'development')}")
    
    success = True
    
    # 1. PostgreSQL 연결 대기
    if not wait_for_postgres():
        success = False
    
    # 2. Redis 연결 대기 (선택사항)
    if not wait_for_redis():
        print("⚠️ Redis 연결 실패 - 캐시 기능이 비활성화됩니다.")
    
    # 3. 데이터베이스 생성
    if success and "postgresql" in DATABASE_URL:
        create_database_if_not_exists()
    
    # 4. 테이블 생성
    if success:
        success = create_tables()
    
    # 5. 인덱스 생성
    if success:
        create_indexes()
    
    # 6. 샘플 데이터 삽입 (개발 환경에서만)
    if success and os.getenv('ENVIRONMENT') == 'development':
        insert_sample_data()
    
    if success:
        print("🎉 초기화 완료!")
        return 0
    else:
        print("❌ 초기화 실패!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
