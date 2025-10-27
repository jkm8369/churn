# OpenAI API 키 설정 가이드

## 🔑 실제 API 키 설정 방법

### 1. OpenAI API 키 발급
1. [OpenAI Platform](https://platform.openai.com/) 접속
2. 계정 로그인 후 API Keys 메뉴 선택
3. "Create new secret key" 클릭
4. 생성된 API 키 복사

### 2. 환경 변수 설정 (Windows PowerShell)
```powershell
# 실제 API 키로 교체
$env:OPENAI_API_KEY="sk-your-actual-api-key-here"

# 서버 실행
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 3. 또는 .env 파일 생성
```bash
# backend 디렉토리에 .env 파일 생성
cd backend
echo "OPENAI_API_KEY=sk-your-actual-api-key-here" > .env

# uvicorn으로 실행 (--env-file 옵션 사용)
uvicorn main:app --host 0.0.0.0 --port 8000 --reload --env-file .env
```

## ✅ 확인 방법
브라우저에서 `http://localhost:3000` 접속 후:
1. "분석 실행" 버튼 클릭
2. 리포트 탭에서 AI 인사이트 확인
3. "API 키를 발급받으세요" 메시지가 사라지고 실제 AI 분석 결과가 표시되면 성공!

## ⚠️ 주의사항
- API 키는 비용이 발생할 수 있으니 사용량을 모니터링하세요
- API 키를 공개 저장소에 올리지 마세요
- 테스트용으로는 GPT-4o-mini 모델을 사용하여 비용을 최소화합니다
