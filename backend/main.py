from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import Iterable, List, Optional
import pandas as pd
import redis
import json
import os
from pydantic import BaseModel
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

from database import get_db, engine, init_db
from models import Event, User, ChurnAnalysis, Base
from schemas import EventCreate, ChurnMetrics, SegmentAnalysis
from analytics import ChurnAnalyzer

app = FastAPI(title="Churn Analysis API", version="1.0.0")

# 데이터베이스 초기화 (시작 시)
@app.on_event("startup")
async def startup_event():
    """서버 시작 시 데이터베이스 테이블 생성"""
    try:
        Base.metadata.create_all(bind=engine)
        print("데이터베이스 테이블 초기화 완료")
    except Exception as e:
        print(f"데이터베이스 초기화 실패: {e}")

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", 
        "http://127.0.0.1:3000",
        "http://localhost:5500", 
        "http://127.0.0.1:5500",
        "*"  # 개발 중에는 모든 origin 허용
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Redis 연결 (환경 변수 기반)
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
try:
    redis_client = redis.from_url(redis_url, decode_responses=True)
    redis_client.ping()  # Redis 연결 테스트
    print(f"Redis 연결 성공: {redis_url}")
except Exception as e:
    print(f"Redis 연결 실패: {e}")
    print("Redis 없이 실행됩니다 (캐싱 비활성화)")
    redis_client = None


DEFAULT_CACHE_PATTERNS: List[str] = [
    "churn_analysis:*",
    "metrics:*",
    "segments:*",
    "trends:*",
    "report:*",
]


def _collect_cache_keys(patterns: Iterable[str]) -> List[str]:
    """지정된 패턴의 캐시 키를 모두 수집"""
    
    if not redis_client:
        return []
    
    keys: List[str] = []
    seen = set()

    for pattern in patterns:
        for key in redis_client.scan_iter(pattern):
            if key not in seen:
                seen.add(key)
                keys.append(key)

    return keys


def invalidate_cache(patterns: Optional[List[str]] = None) -> int:
    """패턴 목록에 해당하는 캐시 키를 삭제하고 삭제된 키 수를 반환"""
    
    if not redis_client:
        return 0

    if patterns is None:
        patterns = DEFAULT_CACHE_PATTERNS

    keys = _collect_cache_keys(patterns)

    if keys:
        redis_client.delete(*keys)

    return len(keys)

class AnalysisRequest(BaseModel):
    start_month: str  # "2025-08" (월 단위) 또는 "2025-08-01" (날짜 단위)
    end_month: str    # "2025-10" (월 단위) 또는 "2025-10-31" (날짜 단위)
    segments: dict = {"gender": True, "age_band": True, "channel": True, "combined": False, "weekday_pattern": False, "time_pattern": False, "action_type": False}
    inactivity_days: List[int] = [30, 60, 90]
    threshold: int = 1  # 최소 이벤트 수 (활성 사용자 기준)

@app.get("/")
async def root():
    return {"message": "Churn Analysis API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """헬스 체크"""
    return {"status": "healthy", "timestamp": datetime.now()}

@app.post("/events/bulk")
async def upload_events(events: List[EventCreate], db: Session = Depends(get_db)):
    """이벤트 데이터 대량 업로드"""
    try:
        db_events = []
        for event_data in events:
            db_event = Event(**event_data.dict())
            db_events.append(db_event)
        
        db.bulk_save_objects(db_events)
        db.commit()
        
        # 캐시 무효화 - 모든 관련 캐시 삭제
        invalidate_cache()
        
        return {"message": f"{len(events)}개 이벤트가 업로드되었습니다."}
    
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/analysis/run")
async def run_analysis(
    request: AnalysisRequest, 
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """이탈 분석 실행"""
    
    # 캐시 키 생성 (세그먼트 설정과 threshold도 포함)
    segments_key = "_".join([f"{k}:{v}" for k, v in sorted(request.segments.items())])
    inactivity_key = "_".join(map(str, sorted(request.inactivity_days)))
    cache_key = f"churn_analysis:{request.start_month}:{request.end_month}:{segments_key}:{inactivity_key}:{request.threshold}"
    
    # 캐시된 결과 확인 (Redis가 있을 때만)
    if redis_client:
        try:
            cached_result = redis_client.get(cache_key)
            if cached_result:
                return json.loads(cached_result)
        except Exception as e:
            print(f"⚠️ Redis 캐시 읽기 실패: {e}")
    
    try:
        # 분석 실행
        analyzer = ChurnAnalyzer(db)
        result = analyzer.run_full_analysis(
            start_month=request.start_month,
            end_month=request.end_month,
            segments=request.segments,
            inactivity_days=request.inactivity_days,
            threshold=request.threshold
        )
        
        # 결과 캐시 (1시간) - Redis가 있을 때만
        if redis_client:
            try:
                redis_client.setex(cache_key, 3600, json.dumps(result, default=str))
            except Exception as e:
                print(f"⚠️ Redis 캐시 쓰기 실패: {e}")
        
        # 백그라운드에서 분석 결과 DB 저장
        background_tasks.add_task(save_analysis_result, result, db)
        
        return result
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"분석 실행 중 오류: {str(e)}")

@app.get("/analysis/metrics")
async def get_metrics(
    month: str,
    db: Session = Depends(get_db)
):
    """월별 주요 지표 조회"""
    
    cache_key = f"metrics:{month}"
    
    # 캐시된 결과 확인 (Redis가 있을 때만)
    if redis_client:
        try:
            cached_result = redis_client.get(cache_key)
            if cached_result:
                return json.loads(cached_result)
        except Exception as e:
            print(f"⚠️ Redis 캐시 읽기 실패: {e}")
    
    try:
        analyzer = ChurnAnalyzer(db)
        metrics = analyzer.get_monthly_metrics(month)
        
        # 캐시 저장 (30분) - Redis가 있을 때만
        if redis_client:
            try:
                redis_client.setex(cache_key, 1800, json.dumps(metrics, default=str))
            except Exception as e:
                print(f"⚠️ Redis 캐시 쓰기 실패: {e}")
        
        return metrics
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analysis/segments")
async def get_segment_analysis(
    start_month: str,
    end_month: str,
    db: Session = Depends(get_db)
):
    """세그먼트별 이탈률 분석"""
    
    cache_key = f"segments:{start_month}:{end_month}"
    
    # 캐시된 결과 확인 (Redis가 있을 때만)
    if redis_client:
        try:
            cached_result = redis_client.get(cache_key)
            if cached_result:
                return json.loads(cached_result)
        except Exception as e:
            print(f"⚠️ Redis 캐시 읽기 실패: {e}")
    
    try:
        analyzer = ChurnAnalyzer(db)
        segments = analyzer.get_segment_analysis(start_month, end_month)
        
        # 캐시 저장 (1시간) - Redis가 있을 때만
        if redis_client:
            try:
                redis_client.setex(cache_key, 3600, json.dumps(segments, default=str))
            except Exception as e:
                print(f"⚠️ Redis 캐시 쓰기 실패: {e}")
        
        return segments
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analysis/trends")
async def get_churn_trends(
    months: List[str],
    db: Session = Depends(get_db)
):
    """월별 이탈률 트렌드"""
    
    cache_key = f"trends:{':'.join(months)}"
    cached_result = redis_client.get(cache_key)
    
    if cached_result:
        return json.loads(cached_result)
    
    try:
        analyzer = ChurnAnalyzer(db)
        trends = analyzer.get_churn_trends(months)
        
        # 캐시 저장 (2시간)
        redis_client.setex(cache_key, 7200, json.dumps(trends, default=str))
        
        return trends
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/inactive")
async def get_inactive_users(
    days: int = 90,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """장기 미접속 사용자 목록"""
    
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # 서브쿼리로 각 사용자의 마지막 활동일 계산
        subquery = db.query(
            Event.user_hash,
            db.func.max(Event.created_at).label('last_activity')
        ).group_by(Event.user_hash).subquery()
        
        # 장기 미접속 사용자 조회
        inactive_users = db.query(subquery).filter(
            subquery.c.last_activity < cutoff_date
        ).limit(limit).all()
        
        result = [
            {
                "user_hash": user.user_hash,
                "last_activity": user.last_activity,
                "inactive_days": (datetime.now() - user.last_activity).days
            }
            for user in inactive_users
        ]
        
        return {"inactive_users": result, "total_count": len(result)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/reports/summary/{month}")
async def get_monthly_report(month: str, db: Session = Depends(get_db)):
    """월별 요약 리포트"""
    
    cache_key = f"report:{month}"
    cached_result = redis_client.get(cache_key)
    
    if cached_result:
        return json.loads(cached_result)
    
    try:
        analyzer = ChurnAnalyzer(db)
        report = analyzer.get_monthly_metrics(month)
        
        # 캐시 저장 (4시간)
        redis_client.setex(cache_key, 14400, json.dumps(report, default=str))
        
        return report
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/cache/clear")
async def clear_cache():
    """캐시 전체 삭제"""
    try:
        deleted_count = invalidate_cache()
        
        return {"message": f"{deleted_count}개 캐시 키가 삭제되었습니다."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def save_analysis_result(result: dict, db: Session):
    """분석 결과를 DB에 저장 (백그라운드 작업)"""
    try:
        config = result.get('config', {})
        metrics = result.get('metrics', {})
        
        analysis_record = ChurnAnalysis(
            analysis_date=datetime.now(),
            start_month=config.get('start_month'),
            end_month=config.get('end_month'),
            total_churn_rate=metrics.get('churn_rate'),
            active_users=metrics.get('active_users'),
            analysis_config=json.dumps(config),
            results=json.dumps(result)
        )
        
        db.add(analysis_record)
        db.commit()
        
    except Exception as e:
        print(f"분석 결과 저장 실패: {e}")
        db.rollback()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
