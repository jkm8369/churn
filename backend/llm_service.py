"""
LLM 기반 인사이트 및 권장 액션 생성 서비스
"""
import os
import json
from typing import Dict, List, Optional
from datetime import datetime
import openai
from openai import OpenAI
import logging

logger = logging.getLogger(__name__)

class LLMInsightGenerator:
    """LLM을 활용한 이탈 분석 인사이트 생성기"""
    
    def __init__(self):
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """OpenAI 클라이언트 초기화"""
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("OPENAI_API_KEY가 설정되지 않았습니다. LLM 기능이 비활성화됩니다.")
            return
        
        try:
            self.client = OpenAI(api_key=api_key)
            logger.info("OpenAI 클라이언트 초기화 완료")
        except Exception as e:
            logger.error(f"OpenAI 클라이언트 초기화 실패: {e}")
    
    def generate_insights_and_actions(self, analysis_data: Dict) -> Dict[str, List[str]]:
        """
        분석 데이터를 바탕으로 LLM을 통해 인사이트와 권장 액션 생성
        
        Args:
            analysis_data: 이탈 분석 결과 데이터
            
        Returns:
            Dict containing 'insights' and 'actions' lists
        """
        if not self.client:
            logger.warning("OpenAI 클라이언트가 초기화되지 않았습니다. 기본 인사이트를 반환합니다.")
            return self._generate_fallback_insights(analysis_data)
        
        try:
            # 데이터 요약 생성
            data_summary = self._create_data_summary(analysis_data)
            
            # LLM 프롬프트 생성
            prompt = self._create_analysis_prompt(data_summary)
            
            # OpenAI API 호출
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",  # 비용 효율적인 모델 사용
                messages=[
                    {
                        "role": "system",
                        "content": self._get_system_prompt()
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                response_format={"type": "json_object"},
                temperature=0.7,
                max_tokens=1500
            )
            
            # 응답 파싱
            result = json.loads(response.choices[0].message.content)
            
            # 결과 검증 및 정제
            insights = result.get('insights', [])[:3]  # 최대 3개
            actions = result.get('actions', [])[:3]    # 최대 3개
            
            # 응답 필터링 및 검증
            insights = self._filter_and_validate_responses(insights, 'insights')
            actions = self._filter_and_validate_responses(actions, 'actions')
            
            logger.info(f"LLM 인사이트 생성 완료: {len(insights)}개 인사이트, {len(actions)}개 액션")
            
            return {
                'insights': insights,
                'actions': actions,
                'generated_by': 'llm',
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"LLM 인사이트 생성 중 오류: {e}")
            return self._generate_fallback_insights(analysis_data)
    
    def _get_system_prompt(self) -> str:
        """시스템 프롬프트 정의"""
        return """당신은 사용자 이탈 분석 전문가입니다. 
주어진 데이터를 분석하여 실용적이고 구체적인 인사이트와 권장 액션을 제공해야 합니다.

응답 규칙:
1. JSON 형식으로만 응답하세요: {"insights": [...], "actions": [...]}
2. 인사이트는 데이터에서 발견된 중요한 패턴이나 트렌드를 설명
3. 권장 액션은 구체적이고 실행 가능한 개선 방안을 제시
4. 각각 최대 3개까지만 제공
5. 한국어로 작성
6. 데이터가 부족하거나 불확실한 경우 "Uncertain" 표기
7. 통계적으로 의미 있는 차이(5%p 이상)만 언급

분석 관점:
- 세그먼트별 이탈률 차이
- 시간별 트렌드 변화
- 재활성화 패턴
- 위험 사용자 그룹
- 데이터 품질 이슈

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

    def _create_data_summary(self, analysis_data: Dict) -> Dict:
        """분석 데이터를 LLM이 이해하기 쉬운 형태로 요약"""
        
        summary = {
            "기본_지표": {},
            "세그먼트_분석": {},
            "트렌드_분석": {},
            "데이터_품질": {},
            "선택된_세그먼트": {}
        }
        
        # 기본 지표 요약
        metrics = analysis_data.get('metrics', {})
        summary["기본_지표"] = {
            "전체_이탈률": f"{metrics.get('churn_rate', 0):.1f}%",
            "활성_사용자": metrics.get('active_users', 0),
            "재활성_사용자": metrics.get('reactivated_users', 0),
            "장기_미접속": metrics.get('long_term_inactive', 0),
            "분석_기간": f"{analysis_data.get('start_month', 'N/A')} ~ {analysis_data.get('end_month', 'N/A')}"
        }
        
        # 선택된 세그먼트 정보 추가
        config = analysis_data.get('config', {})
        selected_segments = config.get('segments', {})
        summary["선택된_세그먼트"] = {
            "성별_분석": selected_segments.get('gender', False),
            "연령대_분석": selected_segments.get('age_band', False),
            "채널_분석": selected_segments.get('channel', False)
        }
        
        # 세그먼트 분석 요약 (선택된 세그먼트만)
        segments = analysis_data.get('segments', {})
        segment_names = {
            'gender': '성별',
            'age_band': '연령대', 
            'channel': '채널'
        }
        
        for segment_type, segment_data in segments.items():
            if segment_data and selected_segments.get(segment_type, False):
                segment_summary = []
                for item in segment_data:
                    segment_summary.append({
                        "그룹": item.get('segment_value', 'Unknown'),
                        "이탈률": f"{item.get('churn_rate', 0):.1f}%",
                        "활성사용자": item.get('current_active', 0),
                        "신뢰도": "Uncertain" if item.get('is_uncertain', False) else "확실"
                    })
                summary["세그먼트_분석"][segment_names.get(segment_type, segment_type)] = segment_summary
        
        # 트렌드 분석 요약
        trends = analysis_data.get('trends', {})
        if trends:
            trend_data = trends.get('monthly_churn_rates', [])
            if len(trend_data) >= 2:
                first_rate = trend_data[0].get('churn_rate', 0)
                last_rate = trend_data[-1].get('churn_rate', 0)
                change = last_rate - first_rate
                
                summary["트렌드_분석"] = {
                    "기간": f"{len(trend_data)}개월",
                    "시작_이탈률": f"{first_rate:.1f}%",
                    "최종_이탈률": f"{last_rate:.1f}%",
                    "변화량": f"{change:+.1f}%p",
                    "트렌드": "상승" if change > 1 else "하락" if change < -1 else "안정"
                }
        
        # 데이터 품질 요약
        quality = analysis_data.get('data_quality', {})
        summary["데이터_품질"] = {
            "총_이벤트": quality.get('total_events', 0),
            "유효_이벤트": quality.get('valid_events', 0),
            "완전성": f"{quality.get('data_completeness', 0):.1f}%",
            "알수없음_비율": f"{quality.get('unknown_ratio', 0):.1f}%"
        }
        
        return summary
    
    def _create_analysis_prompt(self, data_summary: Dict) -> str:
        """LLM 분석을 위한 프롬프트 생성"""
        
        # 선택된 세그먼트 정보 확인
        selected_segments = data_summary.get('선택된_세그먼트', {})
        segment_analysis_available = any(selected_segments.values())
        
        prompt = f"""다음 이탈 분석 데이터를 바탕으로 주요 인사이트 3개와 권장 액션 3개를 생성해주세요.

## 분석 설정

### 선택된 세그먼트
{json.dumps(data_summary['선택된_세그먼트'], ensure_ascii=False, indent=2)}

## 분석 데이터

### 기본 지표
{json.dumps(data_summary['기본_지표'], ensure_ascii=False, indent=2)}"""

        # 세그먼트 분석이 있는 경우만 포함
        if segment_analysis_available and data_summary['세그먼트_분석']:
            prompt += f"""

### 세그먼트별 분석 (선택된 세그먼트만)
{json.dumps(data_summary['세그먼트_분석'], ensure_ascii=False, indent=2)}"""
        else:
            prompt += """

### 세그먼트별 분석
세그먼트 분석이 선택되지 않았습니다. 전체 사용자 기준으로만 분석하세요."""

        prompt += f"""

### 트렌드 분석
{json.dumps(data_summary['트렌드_분석'], ensure_ascii=False, indent=2)}

### 데이터 품질
{json.dumps(data_summary['데이터_품질'], ensure_ascii=False, indent=2)}

## 요청사항

1. **주요 인사이트 3개**: 데이터에서 발견된 가장 중요한 패턴이나 문제점
2. **권장 액션 3개**: 이탈률 개선을 위한 구체적이고 실행 가능한 방안

주의사항:
- 선택되지 않은 세그먼트(성별/연령대/채널)에 대해서는 언급하지 마세요
- 선택된 세그먼트만 분석하고 인사이트를 제공하세요
- 통계적으로 유의미한 차이(5%p 이상)만 언급
- 데이터가 부족한 세그먼트는 "Uncertain" 표기
- 구체적인 수치와 함께 설명
- 실무진이 바로 실행할 수 있는 액션 제시

금지사항:
- 데이터에 없는 정보를 추측하거나 가정하지 마세요
- 개인정보나 민감한 정보를 언급하지 마세요
- 차별적이거나 편향된 분석을 제공하지 마세요
- 법적 조언이나 의료적 조언을 제공하지 마세요
- 과장되거나 부정확한 표현을 사용하지 마세요
- 선택되지 않은 세그먼트의 데이터를 임의로 해석하지 마세요
- 통계적으로 유의미하지 않은 차이를 과장하여 설명하지 마세요
- 불확실한 데이터를 확실한 것처럼 표현하지 마세요"""

        return prompt
    
    def _filter_and_validate_responses(self, responses: List[str], response_type: str) -> List[str]:
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
                logger.warning(f"금지된 용어가 포함된 {response_type} 응답 필터링: {response[:50]}...")
                continue
            
            # 응답 길이 검증 (너무 짧거나 긴 응답 제외)
            if len(response) < 10 or len(response) > 500:
                logger.warning(f"부적절한 길이의 {response_type} 응답 필터링: {len(response)}자")
                continue
            
            # 기본적인 품질 검증 통과
            filtered_responses.append(response.strip())
        
        # 최대 개수 제한
        return filtered_responses[:3]
    
    def _generate_fallback_insights(self, analysis_data: Dict) -> Dict[str, List[str]]:
        """LLM 사용 불가 시 API 키 설정 안내"""
        
        return {
            'insights': [
                "🤖 AI 기반 인사이트를 위해 OpenAI API 키 설정이 필요합니다.",
                "📊 API 키 설정 후 실제 데이터 패턴을 분석한 맞춤형 인사이트를 제공받을 수 있습니다.",
                "⚙️ LLM_INTEGRATION_GUIDE.md 문서를 참조하여 설정을 완료하세요."
            ],
            'actions': [
                "🔑 OpenAI Platform에서 API 키를 발급받으세요.",
                "📁 backend/.env 파일에 OPENAI_API_KEY를 설정하세요.",
                "🔄 서버를 재시작하면 AI 기반 분석이 활성화됩니다."
            ],
            'generated_by': 'api_key_required',
            'timestamp': datetime.now().isoformat(),
            'setup_required': True
        }

# 전역 인스턴스
llm_generator = LLMInsightGenerator()
