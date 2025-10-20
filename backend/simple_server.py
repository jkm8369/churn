"""
간단한 LLM 기반 인사이트 생성 서버 (데이터베이스 없이)
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict
import os
from dotenv import load_dotenv
import json
from datetime import datetime
from analytics import ChurnAnalyzer
from database import get_db

# 환경 변수 로드
load_dotenv()

app = FastAPI(title="Simple Churn Analysis API", version="1.0.0")

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 로컬 테스트용
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AnalysisRequest(BaseModel):
    start_month: str = "2025-08"
    end_month: str = "2025-10"
    segments: dict = {"gender": True, "age_band": True, "channel": True}
    calculated_metrics: dict = None  # 프론트엔드에서 계산된 메트릭

def _filter_responses(responses: List[str]) -> List[str]:
    """응답 필터링 및 검증"""
    if not responses:
        return []
    
    filtered_responses = []
    prohibited_terms = [
        '개인정보', '민감정보', '법적', '의료', '차별', '편향', 
        '추측', '가정', '확실하지', '불확실', '과장'
    ]
    
    for response in responses:
        if not isinstance(response, str) or len(response.strip()) == 0:
            continue
            
        # 금지된 용어가 포함된 응답 필터링
        if any(term in response for term in prohibited_terms):
            print(f"[WARNING] 금지된 용어가 포함된 응답 필터링: {response[:50]}...")
            continue
        
        # 응답 길이 검증 (너무 짧거나 긴 응답 제외)
        if len(response) < 10 or len(response) > 500:
            print(f"[WARNING] 부적절한 길이의 응답 필터링: {len(response)}자")
            continue
        
        # 기본적인 품질 검증 통과
        filtered_responses.append(response.strip())
    
    # 최대 개수 제한
    return filtered_responses[:3]

def get_real_metrics(request: AnalysisRequest) -> Dict:
    """실제 데이터베이스에서 메트릭 계산"""
    try:
        print(f"[DEBUG] get_real_metrics 호출됨: {request.start_month} ~ {request.end_month}")
        print(f"[DEBUG] 세그먼트 설정: {request.segments}")
        
        # 데이터베이스 연결
        db = next(get_db())
        print("[DEBUG] 데이터베이스 연결 성공")
        
        # ChurnAnalyzer 인스턴스 생성
        analyzer = ChurnAnalyzer(db)
        print("[DEBUG] ChurnAnalyzer 생성 완료")
        
        # 실제 분석 실행
        result = analyzer.run_full_analysis(
            start_month=request.start_month,
            end_month=request.end_month,
            segments=request.segments
        )
        print(f"[DEBUG] 분석 결과: {result}")
        
        # 메트릭만 추출
        if "error" in result:
            print(f"[ERROR] 분석 중 오류: {result['error']}")
            # 오류 발생 시 기본값 반환
            return {
                "churn_rate": 0.0,
                "active_users": 0,
                "reactivated_users": 0,
                "long_term_inactive": 0
            }
        
        metrics = result.get("metrics", {})
        print(f"[DEBUG] 추출된 메트릭: {metrics}")
        
        final_metrics = {
            "churn_rate": metrics.get("churn_rate", 0.0),
            "active_users": metrics.get("active_users", 0),
            "reactivated_users": metrics.get("reactivated_users", 0),
            "long_term_inactive": metrics.get("long_term_inactive", 0)
        }
        print(f"[DEBUG] 최종 메트릭: {final_metrics}")
        
        return final_metrics
        
    except Exception as e:
        print(f"[ERROR] 메트릭 계산 오류: {e}")
        import traceback
        traceback.print_exc()
        # 오류 발생 시 기본값 반환
        return {
            "churn_rate": 0.0,
            "active_users": 0,
            "reactivated_users": 0,
            "long_term_inactive": 0
        }

@app.get("/")
async def root():
    return {"message": "Simple Churn Analysis API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now()}

@app.post("/analysis/run")
async def run_analysis(request: AnalysisRequest):
    """LLM 기반 인사이트 생성"""
    
    # OpenAI API 키 확인
    api_key = os.getenv('OPENAI_API_KEY')
    
    if not api_key or api_key == 'your_openai_api_key_here':
        return {
            "analysis_id": f"demo_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "timestamp": datetime.now().isoformat(),
            "insights": [
                "🔑 OpenAI API 키를 설정하면 실제 AI 분석을 경험할 수 있습니다.",
                "📊 현재는 데모 모드로 작동 중입니다.",
                "⚙️ .env 파일에 OPENAI_API_KEY를 설정하고 서버를 재시작하세요."
            ],
            "actions": [
                "🌐 https://platform.openai.com 에서 API 키를 발급받으세요.",
                "📁 backend/.env 파일을 수정하여 API 키를 입력하세요.",
                "🔄 서버를 재시작하면 AI 분석이 활성화됩니다."
            ],
            "llm_metadata": {
                "model_used": None,
                "generation_method": "api_key_required",
                "fallback_used": True,
                "setup_required": True,
                "timestamp": datetime.now().isoformat()
            },
            "metrics": get_real_metrics(request)
        }
    
    # API 키가 있으면 실제 LLM 호출
    try:
        from openai import OpenAI
        
        client = OpenAI(api_key=api_key)
        
        # 실제 세그먼트 데이터 가져오기
        segment_data = []
        
        try:
            db = next(get_db())
            analyzer = ChurnAnalyzer(db)
            
            # 실제 세그먼트 분석 수행
            if request.segments.get("gender", False):
                gender_results = analyzer._analyze_segment("gender", request.start_month, request.end_month)
                if gender_results:
                    gender_text = []
                    for result in gender_results:
                        gender_name = "남성" if result['segment_value'] == 'M' else "여성"
                        gender_text.append(f"{gender_name}: {result['churn_rate']}%")
                    segment_data.append(f"- 성별 이탈률: {', '.join(gender_text)}")
            
            if request.segments.get("age_band", False):
                age_results = analyzer._analyze_segment("age_band", request.start_month, request.end_month)
                if age_results:
                    age_text = []
                    for result in age_results:
                        age_name = f"{result['segment_value']}대"
                        age_text.append(f"{age_name}: {result['churn_rate']}%")
                    segment_data.append(f"- 연령대 이탈률: {', '.join(age_text)}")
            
            if request.segments.get("channel", False):
                channel_results = analyzer._analyze_segment("channel", request.start_month, request.end_month)
                if channel_results:
                    channel_text = []
                    for result in channel_results:
                        channel_name = "웹" if result['segment_value'] == 'web' else "앱"
                        channel_text.append(f"{channel_name}: {result['churn_rate']}%")
                    segment_data.append(f"- 채널 이탈률: {', '.join(channel_text)}")
                    
        except Exception as e:
            print(f"[ERROR] 세그먼트 분석 실패: {e}")
            # 폴백: 기본 메시지
            if any(request.segments.values()):
                segment_data.append("- 세그먼트 분석 중 오류가 발생했습니다.")
        
        segment_section = "\n".join(segment_data) if segment_data else "- 세그먼트 분석이 선택되지 않았습니다."
        
        # 실제 메트릭 가져오기 (프론트엔드에서 전달된 것 우선 사용)
        if request.calculated_metrics:
            real_metrics = request.calculated_metrics
            print(f"[DEBUG] 프론트엔드에서 전달된 메트릭 사용: {real_metrics}")
        else:
            real_metrics = get_real_metrics(request)
            print(f"[DEBUG] 백엔드에서 계산된 메트릭 사용: {real_metrics}")
        
        # 동적 프롬프트 생성
        prompt = f"""다음 이탈 분석 데이터를 바탕으로 주요 인사이트 3개와 권장 액션 3개를 생성해주세요.

## 분석 데이터
- 분석 기간: {request.start_month} ~ {request.end_month}
- 전체 이탈률: {real_metrics['churn_rate']:.1f}%
- 활성 사용자: {real_metrics['active_users']:,}명
- 재활성 사용자: {real_metrics['reactivated_users']:,}명
- 장기 미접속: {real_metrics['long_term_inactive']:,}명

## 세그먼트 분석
{segment_section}

주의사항:
- 선택되지 않은 세그먼트에 대해서는 언급하지 마세요
- 실제 데이터 수치를 기반으로 분석하세요
- 구체적이고 실행 가능한 권장사항을 제시하세요

JSON 형식으로 응답하세요: {{"insights": [...], "actions": [...]}}
"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """당신은 사용자 이탈 분석 전문가입니다. 실용적이고 구체적인 인사이트와 권장 액션을 제공하세요.

절대 하지 말아야 할 것들:
- 추측이나 가정에 기반한 분석 금지
- 데이터에 없는 정보를 임의로 추가하지 말 것
- 개인정보나 민감한 정보 언급 금지
- 비윤리적이거나 차별적인 권장사항 제시 금지
- 법적 조언이나 의료적 조언 제공 금지
- 마케팅이나 영업 목적의 과장된 표현 사용 금지
- 선택되지 않은 세그먼트에 대한 분석 결과 언급 금지
- 통계적으로 유의미하지 않은 차이를 과장하여 설명 금지
- 불확실한 데이터를 확실한 것처럼 표현 금지"""
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ],
            response_format={"type": "json_object"},
            temperature=0.7,
            max_tokens=1000
        )
        
        result = json.loads(response.choices[0].message.content)
        
        # 응답 필터링 및 검증
        insights = _filter_responses(result.get('insights', [])[:3])
        actions = _filter_responses(result.get('actions', [])[:3])
        
        return {
            "analysis_id": f"llm_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "timestamp": datetime.now().isoformat(),
            "insights": insights,
            "actions": actions,
            "llm_metadata": {
                "model_used": "gpt-4o-mini",
                "generation_method": "llm",
                "fallback_used": False,
                "setup_required": False,
                "timestamp": datetime.now().isoformat()
            },
            "metrics": get_real_metrics(request)
        }
        
    except Exception as e:
        return {
            "analysis_id": f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "timestamp": datetime.now().isoformat(),
            "insights": [
                f"AI 분석 중 오류가 발생했습니다: {str(e)}",
                "API 키가 올바른지 확인해주세요.",
                "네트워크 연결을 확인해주세요."
            ],
            "actions": [
                "OpenAI API 키가 유효한지 확인하세요.",
                "인터넷 연결을 확인하세요.",
                "잠시 후 다시 시도해보세요."
            ],
            "llm_metadata": {
                "model_used": None,
                "generation_method": "error",
                "fallback_used": True,
                "setup_required": True,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            },
            "metrics": {
                "churn_rate": 0,
                "active_users": 0,
                "reactivated_users": 0,
                "long_term_inactive": 0
            }
        }

if __name__ == "__main__":
    import uvicorn
    print("Simple Churn Analysis Server 시작...")
    print("http://localhost:8000 에서 API 서버 실행")
    print("AI 분석을 위해 OpenAI API 키를 설정하세요")
    uvicorn.run(app, host="0.0.0.0", port=8000)
