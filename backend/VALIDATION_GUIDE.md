# Analytics.py 계산식 검증 가이드

이 가이드는 `analytics.py`에서 사용되는 계산식들을 검증하고 확인하는 방법을 설명합니다.

## 📊 주요 계산식 정리

### 1. 이탈률(Churn Rate) 계산
```sql
이탈률 = (이탈한 사용자 수 / 이전 월 활성 사용자 수) × 100
```
- **위치**: `get_monthly_metrics()` 함수
- **활성 사용자 정의**: 특정 월에 threshold 이상의 이벤트를 발생시킨 사용자
- **이탈 사용자**: 이전 월에는 활성했지만 현재 월에는 비활성인 사용자

### 2. 유지율(Retention Rate) 계산
```sql
유지율 = (유지된 사용자 수 / 이전 월 활성 사용자 수) × 100
```
- **유지 사용자**: 이전 월과 현재 월 모두 활성인 사용자

### 3. 세그먼트별 이탈률 계산
```sql
세그먼트 이탈률 = (세그먼트별 이탈 사용자 수 / 세그먼트별 이전 월 활성 사용자 수) × 100
```
- **세그먼트**: 성별(gender), 연령대(age_band), 채널(channel)
- **분석 기간**: 전체 기간의 모든 월 전환을 집계

### 4. 장기 미접속 사용자 계산
```sql
장기 미접속 사용자 = 마지막 활동이 N일 이전인 사용자
```
- **기본 기준**: 30일, 60일, 90일
- **계산 방법**: 각 사용자의 마지막 활동 시점과 기준일 비교

### 5. 재활성 사용자 계산
```sql
재활성 사용자 = 현재 월에 활동하지만, 이전에 N일 이상 활동이 없었던 사용자
```
- **기본 간격**: 30일
- **정의**: 30일 이상 비활성 상태였다가 현재 월에 다시 활동한 사용자

### 6. 데이터 품질 지표
```sql
데이터 완성도 = (유효한 이벤트 수 / 전체 이벤트 수) × 100
```
- **유효 이벤트**: user_hash, created_at, action이 모두 존재하는 이벤트

## 🔍 검증 방법

### 1. 단위 테스트 실행

```bash
# 전체 테스트 실행
pytest backend/test_analytics_validation.py -v

# 개별 테스트 실행
pytest backend/test_analytics_validation.py::TestAnalyticsCalculations::test_churn_rate_calculation -v

# 특정 테스트 클래스 실행
pytest backend/test_analytics_validation.py::TestAnalyticsCalculations -v
```

**테스트 시나리오**:
- 기본 이탈률 계산 검증
- 세그먼트별 분석 검증
- 임계값 필터링 검증
- 데이터 품질 계산 검증
- 장기 미접속 사용자 계산 검증
- 재활성 사용자 계산 검증

### 2. 수동 검증 도구 사용

```python
from database import get_db
from calculation_validator import CalculationValidator

# 데이터베이스 연결
db = next(get_db())
validator = CalculationValidator(db)

# 개별 검증
validator.validate_churn_calculation('2024-02', threshold=1)
validator.validate_segment_calculation('gender', '2024-01', '2024-02')
validator.validate_inactivity_calculation('2024-02')

# 전체 검증 리포트 생성
validator.generate_verification_report('2024-02')
```

**검증 기능**:
- 이탈률 계산 단계별 검증
- 세그먼트별 계산 검증
- 장기 미접속 계산 검증
- Analytics 클래스 결과와 직접 계산 결과 비교
- 상세 검증 리포트 생성

### 3. 검증용 샘플 데이터 생성

```python
from database import get_db
from generate_validation_data import ValidationDataGenerator

# 데이터베이스 연결
db = next(get_db())
generator = ValidationDataGenerator(db)

# 모든 시나리오 생성
generator.generate_all_scenarios()

# 개별 시나리오 생성
generator.generate_basic_scenario()
generator.generate_segment_scenario()
generator.generate_threshold_scenario()
```

**생성되는 시나리오**:
- **기본 시나리오**: 간단한 이탈률 계산 검증용
- **임계값 시나리오**: 활성 사용자 임계값 테스트용
- **세그먼트 시나리오**: 세그먼트별 분석 테스트용
- **장기 미접속 시나리오**: 미접속 분석 테스트용
- **재활성 시나리오**: 재활성 사용자 분석 테스트용
- **종합 시나리오**: 모든 기능을 테스트할 수 있는 복합 데이터

### 4. 실제 데이터 벤치마크 테스트

```python
from database import get_db
from benchmark_validation import BenchmarkValidator

# 데이터베이스 연결
db = next(get_db())
validator = BenchmarkValidator(db)

# 종합 벤치마크 실행
validator.run_comprehensive_benchmark('2024-01', '2024-03')

# 개별 벤치마크 실행
validator.benchmark_churn_calculation('2024-02')
validator.benchmark_segment_analysis('gender', '2024-01', '2024-02')
```

**벤치마크 기능**:
- 실제 운영 데이터 사용
- Analytics 클래스 vs 수동 계산 성능 비교
- 계산 결과 정확도 검증
- 데이터 품질 분석
- 종합 벤치마크 리포트 생성

## 📋 검증 체크리스트

### 기본 검증 항목
- [ ] 이탈률 계산이 수학적으로 올바른가?
- [ ] 유지율 계산이 이탈률과 합쳐서 100%가 되는가?
- [ ] 활성 사용자 임계값이 올바르게 적용되는가?
- [ ] 세그먼트별 계산이 전체 계산과 일치하는가?

### 고급 검증 항목
- [ ] 장기 미접속 사용자 계산이 정확한가?
- [ ] 재활성 사용자 정의가 올바른가?
- [ ] 데이터 품질 지표가 의미있게 계산되는가?
- [ ] 월별 트렌드 계산이 일관성 있는가?

### 성능 검증 항목
- [ ] 계산 시간이 합리적인가?
- [ ] 대용량 데이터에서도 정확한가?
- [ ] 메모리 사용량이 적절한가?
- [ ] SQL 쿼리가 효율적인가?

## 🚨 주의사항

### 데이터 품질 고려사항
- **Unknown 값**: 세그먼트 분석에서 제외됨
- **임계값**: 기본값 1 (설정 가능)
- **월 기준**: 매월 1일 기준으로 계산
- **시간대**: UTC 기준 (데이터베이스 설정에 따라 다를 수 있음)

### 계산 정확도
- **허용 오차**: 이탈률/유지율 ±0.1%
- **정수 값**: 완전 일치 요구 (사용자 수 등)
- **반올림**: 소수점 첫째 자리까지 반올림

### 성능 최적화
- **SQL 최적화**: 복잡한 CTE 사용으로 성능 향상
- **인덱스**: created_at, user_hash, segment 컬럼 인덱스 권장
- **배치 처리**: 대용량 데이터는 월별로 분할 처리 권장

## 📊 검증 결과 해석

### 성공적인 검증
- ✅ 모든 계산식이 수학적으로 올바름
- ✅ Analytics 클래스와 수동 계산 결과 일치
- ✅ 성능이 예상 범위 내
- ✅ 데이터 품질이 양호

### 문제가 있는 경우
- ❌ 계산 결과 불일치
- ❌ 성능 문제
- ❌ 데이터 품질 이슈
- ❌ SQL 쿼리 오류

## 🔧 문제 해결

### 일반적인 문제
1. **계산 결과 불일치**: SQL 쿼리와 Python 로직 비교
2. **성능 문제**: 인덱스 추가 또는 쿼리 최적화
3. **데이터 품질**: Unknown 값 처리 방식 확인
4. **메모리 부족**: 배치 처리로 데이터 분할

### 디버깅 팁
- `calculation_validator.py`의 상세 로그 활용
- 단계별 계산 결과 확인
- SQL 쿼리를 직접 실행하여 중간 결과 검증
- 샘플 데이터로 간단한 시나리오 테스트

## 📞 지원

검증 과정에서 문제가 발생하면:
1. 단위 테스트 실행 결과 확인
2. 검증 리포트 상세 분석
3. 데이터 품질 지표 확인
4. 개발팀에 문의

---

이 가이드를 통해 `analytics.py`의 계산식들이 올바르게 작동하는지 확인할 수 있습니다. 정기적인 검증을 통해 데이터 분석의 정확성을 보장하세요.
