from sqlalchemy import Column, Integer, String, DateTime, Float, Text, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()

class Event(Base):
    """사용자 이벤트 테이블"""
    __tablename__ = "events"
    
    id = Column(Integer, primary_key=True, index=True)
    user_hash = Column(String(255), nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, index=True)
    action = Column(String(50), nullable=False)  # 'post', 'comment'
    gender = Column(String(20), default='Unknown')  # 'M', 'F', 'Unknown'
    age_band = Column(String(20), default='Unknown')  # '20s', '30s', etc.
    channel = Column(String(50), default='Unknown')  # 'web', 'app', 'Unknown'
    
    # 메타데이터
    inserted_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # 복합 인덱스
    __table_args__ = (
        Index('idx_user_date', 'user_hash', 'created_at'),
        Index('idx_date_action', 'created_at', 'action'),
        Index('idx_user_gender_age', 'user_hash', 'gender', 'age_band'),
        Index('idx_monthly', 'user_hash', func.date_trunc('month', 'created_at')),
    )

class User(Base):
    """사용자 프로필 테이블 (선택사항)"""
    __tablename__ = "users"
    
    user_hash = Column(String(255), primary_key=True)
    first_seen = Column(DateTime, nullable=False)
    last_seen = Column(DateTime, nullable=False)
    total_events = Column(Integer, default=0)
    
    # 최신 프로필 정보
    current_gender = Column(String(20), default='Unknown')
    current_age_band = Column(String(20), default='Unknown')
    current_channel = Column(String(50), default='Unknown')
    
    # 활동 통계
    total_posts = Column(Integer, default=0)
    total_comments = Column(Integer, default=0)
    active_months = Column(Integer, default=0)
    
    # 이탈 관련
    is_churned = Column(Boolean, default=False)
    churn_date = Column(DateTime, nullable=True)
    days_inactive = Column(Integer, default=0)
    
    # 메타데이터
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

class ChurnAnalysis(Base):
    """이탈 분석 결과 저장 테이블"""
    __tablename__ = "churn_analyses"
    
    id = Column(Integer, primary_key=True, index=True)
    analysis_date = Column(DateTime, nullable=False, index=True)
    
    # 분석 범위
    start_month = Column(String(7), nullable=False)  # 'YYYY-MM'
    end_month = Column(String(7), nullable=False)
    
    # 주요 지표
    total_churn_rate = Column(Float, nullable=True)
    active_users = Column(Integer, nullable=True)
    churned_users = Column(Integer, nullable=True)
    reactivated_users = Column(Integer, nullable=True)
    long_term_inactive = Column(Integer, nullable=True)
    
    # 설정 및 결과 (JSON)
    analysis_config = Column(Text, nullable=True)  # JSON string
    results = Column(Text, nullable=True)  # JSON string
    
    # 실행 정보
    execution_time_seconds = Column(Float, nullable=True)
    status = Column(String(50), default='completed')  # 'running', 'completed', 'failed'
    error_message = Column(Text, nullable=True)
    
    # 메타데이터
    created_at = Column(DateTime, default=func.now())

class MonthlyMetrics(Base):
    """월별 집계 지표 테이블 (성능 최적화용)"""
    __tablename__ = "monthly_metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    year_month = Column(String(7), nullable=False, index=True)  # 'YYYY-MM'
    
    # 기본 지표
    total_users = Column(Integer, nullable=False)
    active_users = Column(Integer, nullable=False)
    new_users = Column(Integer, nullable=False)
    churned_users = Column(Integer, nullable=False)
    retained_users = Column(Integer, nullable=False)
    reactivated_users = Column(Integer, nullable=False)
    
    # 비율
    churn_rate = Column(Float, nullable=True)
    retention_rate = Column(Float, nullable=True)
    
    # 활동 지표
    total_events = Column(Integer, nullable=False)
    total_posts = Column(Integer, nullable=False)
    total_comments = Column(Integer, nullable=False)
    
    # 세그먼트별 지표 (JSON)
    gender_metrics = Column(Text, nullable=True)  # JSON
    age_metrics = Column(Text, nullable=True)     # JSON
    channel_metrics = Column(Text, nullable=True) # JSON
    
    # 메타데이터
    calculated_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # 유니크 제약
    __table_args__ = (
        Index('idx_unique_month', 'year_month', unique=True),
    )

class UserSegment(Base):
    """사용자 세그먼트별 이탈 분석 테이블"""
    __tablename__ = "user_segments"
    
    id = Column(Integer, primary_key=True, index=True)
    year_month = Column(String(7), nullable=False, index=True)
    
    # 세그먼트 정보
    segment_type = Column(String(50), nullable=False)  # 'gender', 'age_band', 'channel'
    segment_value = Column(String(50), nullable=False)  # 'M', '20s', 'web', etc.
    
    # 지표
    total_users = Column(Integer, nullable=False)
    active_users = Column(Integer, nullable=False)
    churned_users = Column(Integer, nullable=False)
    churn_rate = Column(Float, nullable=True)
    
    # 신뢰도 (모수가 적으면 Uncertain)
    is_uncertain = Column(Boolean, default=False)
    sample_size = Column(Integer, nullable=False)
    
    # 메타데이터
    calculated_at = Column(DateTime, default=func.now())
    
    # 복합 인덱스
    __table_args__ = (
        Index('idx_segment_month', 'year_month', 'segment_type', 'segment_value'),
    )

class DataQuality(Base):
    """데이터 품질 모니터링 테이블"""
    __tablename__ = "data_quality"
    
    id = Column(Integer, primary_key=True, index=True)
    check_date = Column(DateTime, nullable=False, index=True)
    
    # 데이터 통계
    total_events = Column(Integer, nullable=False)
    valid_events = Column(Integer, nullable=False)
    invalid_events = Column(Integer, nullable=False)
    duplicate_events = Column(Integer, nullable=False)
    
    # 품질 지표
    data_completeness = Column(Float, nullable=True)  # 완전성 (%)
    data_validity = Column(Float, nullable=True)      # 유효성 (%)
    
    # 이상 탐지
    anomalies_detected = Column(Integer, default=0)
    anomaly_details = Column(Text, nullable=True)  # JSON
    
    # 메타데이터
    created_at = Column(DateTime, default=func.now())

# 뷰 테이블 (읽기 전용)
class UserActivitySummary(Base):
    """사용자 활동 요약 뷰"""
    __tablename__ = "v_user_activity_summary"
    
    user_hash = Column(String(255), primary_key=True)
    first_activity = Column(DateTime)
    last_activity = Column(DateTime)
    total_days_active = Column(Integer)
    total_events = Column(Integer)
    avg_events_per_day = Column(Float)
    most_common_action = Column(String(50))
    most_common_channel = Column(String(50))
    current_status = Column(String(50))  # 'active', 'dormant', 'churned'
    days_since_last_activity = Column(Integer)
    
    # 뷰이므로 실제 테이블 생성하지 않음
    __table_args__ = {'info': {'is_view': True}}
