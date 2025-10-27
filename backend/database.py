from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
import os
from urllib.parse import quote_plus

# 환경 변수에서 데이터베이스 설정 읽기
# 기본적으로 SQLite 사용 (개발 환경)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///./churn_analysis.db"
)

# 프로덕션 환경에서는 MySQL 사용
if os.getenv("ENVIRONMENT") == "production":
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "mysql+pymysql://churn_user:churn_password@localhost:3306/churn_analysis"
    )

# SQLAlchemy 엔진 생성
if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=os.getenv("SQL_ECHO", "false").lower() == "true"
    )
else:
    engine = create_engine(
        DATABASE_URL,
        pool_size=20,
        max_overflow=30,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=os.getenv("SQL_ECHO", "false").lower() == "true"
    )

# 세션 팩토리
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base 클래스
Base = declarative_base()

# 의존성 주입용 DB 세션
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 데이터베이스 초기화
def init_db():
    """데이터베이스 테이블 생성"""
    from models import Base
    Base.metadata.create_all(bind=engine)

# 데이터베이스 연결 테스트
def test_connection():
    """데이터베이스 연결 테스트"""
    try:
        with engine.connect() as connection:
            result = connection.execute("SELECT 1")
            return True
    except Exception as e:
        print(f"데이터베이스 연결 실패: {e}")
        return False
