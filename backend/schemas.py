from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum

# Enum 정의
class ActionType(str, Enum):
    post = "post"
    comment = "comment"

class GenderType(str, Enum):
    M = "M"
    F = "F"
    Unknown = "Unknown"

class ChannelType(str, Enum):
    web = "web"
    app = "app"
    Unknown = "Unknown"

class AnalysisStatus(str, Enum):
    running = "running"
    completed = "completed"
    failed = "failed"

# 요청 스키마
class EventCreate(BaseModel):
    user_hash: str = Field(..., min_length=1, max_length=255)
    created_at: datetime
    action: ActionType
    gender: GenderType = GenderType.Unknown
    age_band: str = Field(default="Unknown", max_length=20)
    channel: ChannelType = ChannelType.Unknown

    @validator('user_hash')
    def validate_user_hash(cls, v):
        if not v or v.strip() == "":
            raise ValueError('user_hash cannot be empty')
        return v.strip()

    @validator('age_band')
    def validate_age_band(cls, v):
        valid_ages = ['10s', '20s', '30s', '40s', '50s', '60s', '70s', 'Unknown']
        if v not in valid_ages:
            return 'Unknown'
        return v

class AnalysisRequest(BaseModel):
    start_month: str = Field(..., regex=r'^\d{4}-\d{2}$')
    end_month: str = Field(..., regex=r'^\d{4}-\d{2}$')
    segments: Dict[str, bool] = Field(default={
        "gender": True,
        "age_band": True,
        "channel": True
    })
    inactivity_days: List[int] = Field(default=[30, 60, 90])

    @validator('end_month')
    def validate_month_range(cls, v, values):
        if 'start_month' in values and v < values['start_month']:
            raise ValueError('end_month must be after start_month')
        return v

    @validator('inactivity_days')
    def validate_inactivity_days(cls, v):
        if not v or len(v) == 0:
            return [30, 60, 90]
        return sorted(list(set(v)))  # 중복 제거 및 정렬

# 응답 스키마
class ChurnMetrics(BaseModel):
    month: str
    active_users: int
    previous_active_users: int
    churned_users: int
    retained_users: int
    churn_rate: float
    retention_rate: float
    reactivated_users: int
    long_term_inactive: int
    month_over_month_change: Dict[str, Any]

class SegmentAnalysis(BaseModel):
    segment_value: str
    current_active: int
    previous_active: int
    churned_users: int
    churn_rate: float
    is_uncertain: bool

class TrendData(BaseModel):
    month: str
    churn_rate: float
    active_users: int
    churned_users: int

class ChurnTrends(BaseModel):
    months: List[str]
    trends: List[TrendData]

class InactivityAnalysis(BaseModel):
    inactive_30d: int = 0
    inactive_60d: int = 0
    inactive_90d: int = 0

class ReactivationAnalysis(BaseModel):
    reactivated_users: int
    gap_days: int

class DataQuality(BaseModel):
    total_events: int
    valid_events: int
    invalid_events: int
    unknown_values: int
    unique_users: int
    data_completeness: float
    unknown_ratio: float

class AnalysisResult(BaseModel):
    analysis_id: str
    timestamp: datetime
    config: AnalysisRequest
    metrics: ChurnMetrics
    trends: ChurnTrends
    segments: Dict[str, List[SegmentAnalysis]]
    inactivity: InactivityAnalysis
    reactivation: ReactivationAnalysis
    insights: List[str]
    actions: List[str]
    data_quality: DataQuality
    execution_time_seconds: float

class InactiveUser(BaseModel):
    user_hash: str
    last_activity: datetime
    inactive_days: int

class InactiveUsersResponse(BaseModel):
    inactive_users: List[InactiveUser]
    total_count: int

class MonthlyReport(BaseModel):
    month: str
    generated_at: datetime
    summary: Dict[str, Any]
    insights: List[str]
    actions: List[str]
    data_quality: DataQuality
    charts_data: Dict[str, Any]

class HealthCheck(BaseModel):
    status: str
    timestamp: datetime
    version: str = "1.0.0"
    database_connected: bool = True
    redis_connected: bool = True

class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    timestamp: datetime
    request_id: Optional[str] = None

class SuccessResponse(BaseModel):
    message: str
    data: Optional[Dict[str, Any]] = None
    timestamp: datetime

class CacheInfo(BaseModel):
    keys_deleted: int
    message: str

# 데이터베이스 모델 응답 스키마
class EventResponse(BaseModel):
    id: int
    user_hash: str
    created_at: datetime
    action: str
    gender: str
    age_band: str
    channel: str
    inserted_at: datetime

    class Config:
        from_attributes = True

class UserResponse(BaseModel):
    user_hash: str
    first_seen: datetime
    last_seen: datetime
    total_events: int
    current_gender: str
    current_age_band: str
    current_channel: str
    total_posts: int
    total_comments: int
    active_months: int
    is_churned: bool
    churn_date: Optional[datetime]
    days_inactive: int

    class Config:
        from_attributes = True

class ChurnAnalysisResponse(BaseModel):
    id: int
    analysis_date: datetime
    start_month: str
    end_month: str
    total_churn_rate: Optional[float]
    active_users: Optional[int]
    churned_users: Optional[int]
    reactivated_users: Optional[int]
    long_term_inactive: Optional[int]
    execution_time_seconds: Optional[float]
    status: str

    class Config:
        from_attributes = True

# 배치 업로드 응답
class BulkUploadResponse(BaseModel):
    message: str
    total_events: int
    successful_events: int
    failed_events: int
    errors: List[str] = []
    execution_time_seconds: float

# 페이지네이션
class PaginationParams(BaseModel):
    page: int = Field(default=1, ge=1)
    size: int = Field(default=100, ge=1, le=1000)

class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    size: int
    pages: int

# 필터링 파라미터
class EventFilters(BaseModel):
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    user_hash: Optional[str] = None
    action: Optional[ActionType] = None
    gender: Optional[GenderType] = None
    age_band: Optional[str] = None
    channel: Optional[ChannelType] = None

class AnalysisFilters(BaseModel):
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    status: Optional[AnalysisStatus] = None
    min_execution_time: Optional[float] = None
    max_execution_time: Optional[float] = None
