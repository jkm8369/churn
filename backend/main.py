from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import List, Optional
import pandas as pd
import redis
import json
from pydantic import BaseModel

from database import get_db, engine
from models import Event, User, ChurnAnalysis
from schemas import EventCreate, ChurnMetrics, SegmentAnalysis
from analytics import ChurnAnalyzer

app = FastAPI(title="Churn Analysis API", version="1.0.0")

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:5500"],  # 프론트엔드 도메인
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis 연결
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

class AnalysisRequest(BaseModel):
    start_month: str  # "2025-08" (월 단위) 또는 "2025-08-01" (날짜 단위)
    end_month: str    # "2025-10" (월 단위) 또는 "2025-10-31" (날짜 단위)
    segments: dict = {"gender": True, "age_band": True, "channel": True}
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
        
        # 캐시 무효화
        redis_client.delete("churn_analysis:*")
        
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
    
    # 캐시된 결과 확인
    cached_result = redis_client.get(cache_key)
    if cached_result:
        return json.loads(cached_result)
    
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
        
        # 결과 캐시 (1시간)
        redis_client.setex(cache_key, 3600, json.dumps(result, default=str))
        
        # 백그라운드에서 분석 결과 DB 저장
        background_tasks.add_task(save_analysis_result, result, db)
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"분석 실행 중 오류: {str(e)}")

@app.get("/analysis/metrics")
async def get_metrics(
    month: str,
    db: Session = Depends(get_db)
):
    """월별 주요 지표 조회"""
    
    cache_key = f"metrics:{month}"
    cached_result = redis_client.get(cache_key)
    
    if cached_result:
        return json.loads(cached_result)
    
    try:
        analyzer = ChurnAnalyzer(db)
        metrics = analyzer.get_monthly_metrics(month)
        
        # 캐시 저장 (30분)
        redis_client.setex(cache_key, 1800, json.dumps(metrics, default=str))
        
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
    cached_result = redis_client.get(cache_key)
    
    if cached_result:
        return json.loads(cached_result)
    
    try:
        analyzer = ChurnAnalyzer(db)
        segments = analyzer.get_segment_analysis(start_month, end_month)
        
        # 캐시 저장 (1시간)
        redis_client.setex(cache_key, 3600, json.dumps(segments, default=str))
        
        return segments
        
    except Exception as e:
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
        report = analyzer.generate_monthly_report(month)
        
        # 캐시 저장 (4시간)
        redis_client.setex(cache_key, 14400, json.dumps(report, default=str))
        
        return report
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/cache/clear")
async def clear_cache():
    """캐시 전체 삭제"""
    try:
        keys = redis_client.keys("churn_analysis:*")
        keys.extend(redis_client.keys("metrics:*"))
        keys.extend(redis_client.keys("segments:*"))
        keys.extend(redis_client.keys("trends:*"))
        keys.extend(redis_client.keys("report:*"))
        
        if keys:
            redis_client.delete(*keys)
        
        return {"message": f"{len(keys)}개 캐시 키가 삭제되었습니다."}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def save_analysis_result(result: dict, db: Session):
    """분석 결과를 DB에 저장 (백그라운드 작업)"""
    try:
        analysis_record = ChurnAnalysis(
            analysis_date=datetime.now(),
            start_month=result.get('start_month'),
            end_month=result.get('end_month'),
            total_churn_rate=result.get('metrics', {}).get('churn_rate'),
            active_users=result.get('metrics', {}).get('active_users'),
            analysis_config=json.dumps(result.get('config', {})),
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
