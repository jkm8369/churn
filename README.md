# 이탈자 분석 시스템 (Churn Analysis Dashboard)

# 🤖 **AI-First 이탈자 분석 시스템**

> **완전 LLM 기반 인사이트 생성** - 기존 하드코딩된 분석을 GPT 모델로 완전 대체

## 🚀 빠른 시작 (로컬 테스트)

### 1. 현재 디렉토리에서 바로 실행
```bash
# 1. 웹 서버 실행 (Python 내장 서버 사용)
python -m http.server 8080

# 또는 Node.js가 있다면
npx serve . -p 8080

# 또는 Live Server (VS Code 확장) 사용
```

### 2. 브라우저에서 접속
```
http://localhost:8080
```

### 3. 사용법 (로컬 환경)
1. 페이지 로드 시 샘플 데이터 자동 로드
2. "분석 실행" 버튼 클릭
3. 실시간 진행률 확인
4. 대시보드에서 결과 확인
5. **리포트 탭에서 AI 설정 안내 확인** 🤖

> ⚠️ **로컬 환경에서는 AI 분석이 제한됩니다.** 완전한 AI 기능을 위해서는 Docker 환경 + OpenAI API 키가 필요합니다.

---

## 🐳 프로덕션 시스템 실행 (Docker)

### 사전 요구사항
- Docker Desktop 설치
- Docker Compose 설치
- 최소 4GB RAM 권장
- **OpenAI API 키** (AI 인사이트 기능 사용 시)

### 1. 전체 시스템 실행
```bash
# OpenAI API 키 설정 (필수)
export OPENAI_API_KEY=your_openai_api_key_here

# 모든 서비스 실행 (PostgreSQL + Redis + API + Frontend)
docker-compose up -d

# 로그 확인 (AI 분석 상태 포함)
docker-compose logs -f backend
```

### 2. 서비스 확인
```bash
# API 서버 헬스체크
curl http://localhost:8000/health

# 프론트엔드 접속
curl http://localhost/

# 데이터베이스 연결 확인
docker exec -it churn_postgres psql -U churn_user -d churn_analysis
```

### 3. 개별 서비스 관리
```bash
# 특정 서비스만 재시작
docker-compose restart backend

# 로그 확인
docker-compose logs backend

# 서비스 중지
docker-compose down
```

---

## 🤖 AI 인사이트 설정 (선택사항)

### OpenAI API 키 설정
```bash
# backend 디렉토리에 .env 파일 생성
cd backend
echo "OPENAI_API_KEY=your_openai_api_key_here" > .env

# 또는 Docker 환경 변수로 설정
export OPENAI_API_KEY=your_openai_api_key_here
docker-compose up -d
```

### AI 기능 확인
```bash
# AI 분석 상태 확인 (리포트 탭 하단)
# ✅ AI 분석 완료: GPT 모델 사용
# ⚠️ AI 분석 실패: 기본 분석 사용 (API 키 없음)
```

**📋 자세한 설정 방법**: [LLM_INTEGRATION_GUIDE.md](./LLM_INTEGRATION_GUIDE.md) 참조

---

## 📊 API 사용법

### 데이터 업로드
```bash
curl -X POST "http://localhost:8000/events/bulk" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "user_hash": "user001",
      "created_at": "2025-10-10T14:30:00Z",
      "action": "post",
      "gender": "F",
      "age_band": "30s",
      "channel": "app"
    }
  ]'
```

### 분석 실행
```bash
curl -X POST "http://localhost:8000/analysis/run" \
  -H "Content-Type: application/json" \
  -d '{
    "start_month": "2025-08",
    "end_month": "2025-10",
    "segments": {
      "gender": true,
      "age_band": true,
      "channel": true
    }
  }'
```

### 결과 조회
```bash
# 월별 지표
curl "http://localhost:8000/analysis/metrics?month=2025-10"

# 세그먼트 분석
curl "http://localhost:8000/analysis/segments?start_month=2025-08&end_month=2025-10"

# 이탈률 트렌드
curl "http://localhost:8000/analysis/trends?months=2025-08,2025-09,2025-10"
```

---

## 🔧 개발 환경 설정

### 백엔드 개발
```bash
cd backend

# 가상환경 생성
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt

# 개발 서버 실행
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 프론트엔드 개발
```bash
# 라이브 서버로 실행 (파일 변경 시 자동 새로고침)
npx live-server --port=3000 --cors

# 또는 Python 서버
python -m http.server 3000
```

---

## 📁 프로젝트 구조

```
main_churn/
├── index.html              # 메인 프론트엔드
├── styles.css              # CSS 스타일
├── script.js               # 기본 JavaScript (로컬용)
├── api-client.js           # API 연동 JavaScript (프로덕션용)
├── data/
│   └── events.csv          # 샘플 데이터
├── backend/
│   ├── main.py             # FastAPI 서버
│   ├── models.py           # 데이터베이스 모델
│   ├── analytics.py        # 분석 엔진
│   ├── database.py         # DB 연결 설정
│   └── requirements.txt    # Python 의존성
├── docker-compose.yml      # Docker 구성
└── README.md              # 이 파일
```

---

## 🎯 사용 시나리오

### 시나리오 1: 빠른 데모
1. `python -m http.server 8080` 실행
2. `http://localhost:8080` 접속
3. "분석 실행" 클릭
4. 결과 확인

### 시나리오 2: 실제 데이터 분석
1. Docker 시스템 실행
2. CSV 파일 업로드
3. API로 분석 실행
4. 대시보드에서 실시간 결과 확인

### 시나리오 3: 개발/커스터마이징
1. 백엔드 개발 환경 설정
2. 프론트엔드 라이브 서버 실행
3. 코드 수정 및 테스트
4. Docker로 프로덕션 배포

---

## 🚨 문제 해결

### 포트 충돌
```bash
# 사용 중인 포트 확인
netstat -an | findstr :8000
netstat -an | findstr :5432

# 포트 변경
docker-compose.yml에서 포트 수정
```

### 데이터베이스 연결 오류
```bash
# 컨테이너 상태 확인
docker-compose ps

# 데이터베이스 로그 확인
docker-compose logs postgres

# 데이터베이스 재시작
docker-compose restart postgres
```

### 메모리 부족
```bash
# 사용하지 않는 컨테이너 정리
docker system prune

# 메모리 사용량 확인
docker stats
```

---

## 📞 지원

- 이슈 발생 시: 로그 파일 확인 (`logs/` 디렉토리)
- 성능 문제: Redis 캐시 상태 확인
- 데이터 문제: PostgreSQL 연결 및 테이블 상태 확인
