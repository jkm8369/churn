"""
실제 데이터와 계산 결과 비교를 위한 벤치마크 테스트
이 스크립트는 실제 운영 데이터를 사용하여 analytics.py의 계산 결과를 검증합니다.
"""

from datetime import datetime
from sqlalchemy import text
from analytics import ChurnAnalyzer
import json
import time
from typing import Dict, List

class BenchmarkValidator:
    """실제 데이터 벤치마크 검증기"""
    
    def __init__(self, db_session):
        self.db = db_session
        self.analyzer = ChurnAnalyzer(db_session)
        
        # 데이터베이스 타입 확인
        from database import DATABASE_URL
        self.is_sqlite = DATABASE_URL.startswith('sqlite')
        self.is_mysql = 'mysql' in DATABASE_URL.lower()
    
    def _get_month_trunc(self, column_name: str = 'created_at') -> str:
        """데이터베이스별로 적절한 월 추출 SQL 반환"""
        if self.is_sqlite:
            return f"strftime('%Y-%m', {column_name})"
        elif self.is_mysql:
            return f"DATE_FORMAT({column_name}, '%Y-%m')"
        else:  # 기본값은 SQLite
            return f"strftime('%Y-%m', {column_name})"
    
    def get_data_statistics(self, start_month: str, end_month: str) -> Dict:
        """데이터 통계 정보 조회"""
        
        print("📊 데이터 통계 조회 중...")
        
        query = text(f"""
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT user_hash) as unique_users,
            COUNT(DISTINCT {self._get_month_trunc('created_at')}) as months_covered,
            MIN(created_at) as earliest_event,
            MAX(created_at) as latest_event,
            COUNT(CASE WHEN gender IS NOT NULL AND gender != 'Unknown' THEN 1 END) as gender_known,
            COUNT(CASE WHEN age_band IS NOT NULL AND age_band != 'Unknown' THEN 1 END) as age_known,
            COUNT(CASE WHEN channel IS NOT NULL AND channel != 'Unknown' THEN 1 END) as channel_known
        FROM events
        WHERE {self._get_month_trunc('created_at')} BETWEEN :start_month AND :end_month
        """)
        
        result = self.db.execute(query, {
            "start_month": f"{start_month}-01",
            "end_month": f"{end_month}-01"
        }).fetchone()
        
        stats = {
            'total_events': result.total_events,
            'unique_users': result.unique_users,
            'months_covered': result.months_covered,
            'earliest_event': result.earliest_event.isoformat() if result.earliest_event else None,
            'latest_event': result.latest_event.isoformat() if result.latest_event else None,
            'gender_completeness': (result.gender_known / result.total_events * 100) if result.total_events > 0 else 0,
            'age_completeness': (result.age_known / result.total_events * 100) if result.total_events > 0 else 0,
            'channel_completeness': (result.channel_known / result.total_events * 100) if result.total_events > 0 else 0,
        }
        
        print("✅ 데이터 통계 조회 완료")
        print(f"   총 이벤트: {stats['total_events']:,}개")
        print(f"   고유 사용자: {stats['unique_users']:,}명")
        print(f"   분석 기간: {stats['months_covered']}개월")
        print(f"   데이터 완성도: 성별 {stats['gender_completeness']:.1f}%, 연령 {stats['age_completeness']:.1f}%, 채널 {stats['channel_completeness']:.1f}%")
        
        return stats
    
    def benchmark_churn_calculation(self, month: str, threshold: int = 1) -> Dict:
        """이탈률 계산 벤치마크"""
        
        print(f"🏃‍♂️ 이탈률 계산 벤치마크 - {month}월 (임계값: {threshold})")
        print("-" * 60)
        
        # Analytics 클래스 실행
        start_time = time.time()
        analytics_result = self.analyzer.get_monthly_metrics(month, threshold)
        analytics_time = time.time() - start_time
        
        # 수동 계산 (직접 SQL)
        start_time = time.time()
        manual_result = self._manual_churn_calculation(month, threshold)
        manual_time = time.time() - start_time
        
        # 결과 비교
        comparison = self._compare_churn_results(analytics_result, manual_result)
        
        benchmark = {
            'month': month,
            'threshold': threshold,
            'analytics_result': analytics_result,
            'manual_result': manual_result,
            'comparison': comparison,
            'performance': {
                'analytics_time': analytics_time,
                'manual_time': manual_time,
                'speed_ratio': manual_time / analytics_time if analytics_time > 0 else 0
            }
        }
        
        print(f"⏱️ 성능 비교:")
        print(f"   Analytics 클래스: {analytics_time:.3f}초")
        print(f"   수동 계산: {manual_time:.3f}초")
        print(f"   속도 비율: {benchmark['performance']['speed_ratio']:.2f}x")
        
        print("✅ 벤치마크 완료: " + ("성공" if comparison['is_valid'] else "실패"))
        
        return benchmark
    
    def _manual_churn_calculation(self, month: str, threshold: int) -> Dict:
        """수동 이탈률 계산 (직접 SQL)"""
        
        previous_month = self.analyzer._get_previous_month(month)
        
        # 직접 SQL로 계산
        query = text(f"""
        WITH monthly_users AS (
            SELECT 
                {self._get_month_trunc('created_at')} as month,
                user_hash,
                COUNT(*) as event_count
            FROM events 
            WHERE {self._get_month_trunc('created_at')} IN (:prev_month, :curr_month)
            GROUP BY {self._get_month_trunc('created_at')}, user_hash
            HAVING COUNT(*) >= :threshold
        ),
        current_active AS (
            SELECT user_hash FROM monthly_users 
            WHERE month = :curr_month
        ),
        previous_active AS (
            SELECT user_hash FROM monthly_users 
            WHERE month = :prev_month
        ),
        churned AS (
            SELECT p.user_hash 
            FROM previous_active p
            LEFT JOIN current_active c ON p.user_hash = c.user_hash
            WHERE c.user_hash IS NULL
        ),
        retained AS (
            SELECT p.user_hash
            FROM previous_active p
            INNER JOIN current_active c ON p.user_hash = c.user_hash
        )
        SELECT 
            (SELECT COUNT(*) FROM current_active) as current_active_users,
            (SELECT COUNT(*) FROM previous_active) as previous_active_users,
            (SELECT COUNT(*) FROM churned) as churned_users,
            (SELECT COUNT(*) FROM retained) as retained_users
        """)
        
        result = self.db.execute(query, {
            "curr_month": f"{month}-01",
            "prev_month": f"{previous_month}-01",
            "threshold": threshold
        }).fetchone()
        
        if not result:
            return {"error": "데이터를 찾을 수 없습니다."}
        
        current_active = result.current_active_users or 0
        previous_active = result.previous_active_users or 0
        churned = result.churned_users or 0
        retained = result.retained_users or 0
        
        churn_rate = (churned / previous_active * 100) if previous_active > 0 else 0
        retention_rate = (retained / previous_active * 100) if previous_active > 0 else 0
        
        return {
            "month": month,
            "active_users": current_active,
            "previous_active_users": previous_active,
            "churned_users": churned,
            "retained_users": retained,
            "churn_rate": round(churn_rate, 1),
            "retention_rate": round(retention_rate, 1)
        }
    
    def _compare_churn_results(self, analytics_result: Dict, manual_result: Dict) -> Dict:
        """이탈률 계산 결과 비교"""
        
        if 'error' in analytics_result or 'error' in manual_result:
            return {
                'is_valid': False,
                'error': '계산 중 오류 발생',
                'analytics_error': analytics_result.get('error'),
                'manual_error': manual_result.get('error')
            }
        
        # 주요 지표 비교
        comparisons = {
            'active_users': {
                'analytics': analytics_result.get('active_users', 0),
                'manual': manual_result.get('active_users', 0),
                'difference': 0,
                'is_match': True
            },
            'previous_active_users': {
                'analytics': analytics_result.get('previous_active_users', 0),
                'manual': manual_result.get('previous_active_users', 0),
                'difference': 0,
                'is_match': True
            },
            'churned_users': {
                'analytics': analytics_result.get('churned_users', 0),
                'manual': manual_result.get('churned_users', 0),
                'difference': 0,
                'is_match': True
            },
            'retained_users': {
                'analytics': analytics_result.get('retained_users', 0),
                'manual': manual_result.get('retained_users', 0),
                'difference': 0,
                'is_match': True
            },
            'churn_rate': {
                'analytics': analytics_result.get('churn_rate', 0),
                'manual': manual_result.get('churn_rate', 0),
                'difference': 0,
                'is_match': True
            },
            'retention_rate': {
                'analytics': analytics_result.get('retention_rate', 0),
                'manual': manual_result.get('retention_rate', 0),
                'difference': 0,
                'is_match': True
            }
        }
        
        # 차이 계산 및 매치 여부 확인
        is_all_valid = True
        
        for key, comp in comparisons.items():
            comp['difference'] = abs(comp['analytics'] - comp['manual'])
            
            # 허용 오차 설정
            if key in ['churn_rate', 'retention_rate']:
                tolerance = 0.1  # 0.1% 허용 오차
            else:
                tolerance = 0  # 정수 값은 완전 일치 요구
            
            comp['is_match'] = comp['difference'] <= tolerance
            is_all_valid = is_all_valid and comp['is_match']
        
        return {
            'is_valid': is_all_valid,
            'comparisons': comparisons,
            'summary': {
                'total_metrics': len(comparisons),
                'matching_metrics': sum(1 for comp in comparisons.values() if comp['is_match']),
                'accuracy_rate': sum(1 for comp in comparisons.values() if comp['is_match']) / len(comparisons) * 100
            }
        }
    
    def benchmark_segment_analysis(self, segment_type: str, start_month: str, end_month: str) -> Dict:
        """세그먼트 분석 벤치마크"""
        
        print(f"🏃‍♂️ 세그먼트 분석 벤치마크 - {segment_type} ({start_month} ~ {end_month})")
        print("-" * 60)
        
        # Analytics 클래스 실행
        start_time = time.time()
        analytics_result = self.analyzer._analyze_segment(segment_type, start_month, end_month)
        analytics_time = time.time() - start_time
        
        # 수동 계산
        start_time = time.time()
        manual_result = self._manual_segment_calculation(segment_type, start_month, end_month)
        manual_time = time.time() - start_time
        
        # 결과 비교
        comparison = self._compare_segment_results(analytics_result, manual_result)
        
        benchmark = {
            'segment_type': segment_type,
            'start_month': start_month,
            'end_month': end_month,
            'analytics_result': analytics_result,
            'manual_result': manual_result,
            'comparison': comparison,
            'performance': {
                'analytics_time': analytics_time,
                'manual_time': manual_time,
                'speed_ratio': manual_time / analytics_time if analytics_time > 0 else 0
            }
        }
        
        print(f"⏱️ 성능 비교:")
        print(f"   Analytics 클래스: {analytics_time:.3f}초")
        print(f"   수동 계산: {manual_time:.3f}초")
        print(f"   속도 비율: {benchmark['performance']['speed_ratio']:.2f}x")
        
        print("✅ 벤치마크 완료: " + ("성공" if comparison['is_valid'] else "실패"))
        
        return benchmark
    
    def _manual_segment_calculation(self, segment_type: str, start_month: str, end_month: str) -> List[Dict]:
        """수동 세그먼트 계산"""
        
        # 간단한 세그먼트 계산 (Analytics 클래스의 복잡한 로직을 단순화)
        query = text(f"""
        WITH segment_monthly AS (
            SELECT 
                {segment_type} AS segment_value,
                {self._get_month_trunc('created_at')} AS month,
                user_hash
            FROM events 
            WHERE {self._get_month_trunc('created_at')} BETWEEN :start_month AND :end_month
              AND {segment_type} IS NOT NULL 
              AND {segment_type} != 'Unknown'
            GROUP BY {segment_type}, {self._get_month_trunc('created_at')}, user_hash
        )
        SELECT 
            segment_value,
            COUNT(DISTINCT user_hash) as total_users,
            COUNT(DISTINCT CASE WHEN month = :end_month THEN user_hash END) as current_users,
            COUNT(DISTINCT CASE WHEN month = :start_month THEN user_hash END) as start_users
        FROM segment_monthly
        GROUP BY segment_value
        ORDER BY total_users DESC
        """)
        
        results = self.db.execute(query, {
            "start_month": f"{start_month}-01",
            "end_month": f"{end_month}-01"
        }).fetchall()
        
        return [
            {
                "segment_value": row.segment_value,
                "current_active": row.current_users,
                "previous_active": row.start_users,
                "churned_users": 0,  # 단순화된 계산
                "churn_rate": 0.0,   # 단순화된 계산
                "is_uncertain": False
            }
            for row in results
        ]
    
    def _compare_segment_results(self, analytics_result: List[Dict], manual_result: List[Dict]) -> Dict:
        """세그먼트 분석 결과 비교"""
        
        # 세그먼트별로 결과 매핑
        analytics_dict = {r['segment_value']: r for r in analytics_result}
        manual_dict = {r['segment_value']: r for r in manual_result}
        
        all_segments = set(analytics_dict.keys()) | set(manual_dict.keys())
        
        comparisons = {}
        is_all_valid = True
        
        for segment in all_segments:
            analytics = analytics_dict.get(segment, {})
            manual = manual_dict.get(segment, {})
            
            comparison = {
                'segment': segment,
                'analytics_current': analytics.get('current_active', 0),
                'manual_current': manual.get('current_active', 0),
                'analytics_previous': analytics.get('previous_active', 0),
                'manual_previous': manual.get('previous_active', 0),
                'is_match': True
            }
            
            # 현재 활성 사용자 수 비교
            current_match = comparison['analytics_current'] == comparison['manual_current']
            previous_match = comparison['analytics_previous'] == comparison['manual_previous']
            
            comparison['is_match'] = current_match and previous_match
            is_all_valid = is_all_valid and comparison['is_match']
            
            comparisons[segment] = comparison
        
        return {
            'is_valid': is_all_valid,
            'comparisons': comparisons,
            'summary': {
                'total_segments': len(all_segments),
                'matching_segments': sum(1 for comp in comparisons.values() if comp['is_match']),
                'accuracy_rate': sum(1 for comp in comparisons.values() if comp['is_match']) / len(all_segments) * 100 if all_segments else 100
            }
        }
    
    def run_comprehensive_benchmark(self, start_month: str, end_month: str, threshold: int = 1) -> Dict:
        """종합 벤치마크 실행"""
        
        print("🚀 종합 벤치마크 시작")
        print("=" * 80)
        
        benchmark_results = {
            'timestamp': datetime.now().isoformat(),
            'config': {
                'start_month': start_month,
                'end_month': end_month,
                'threshold': threshold
            },
            'data_statistics': {},
            'benchmarks': {},
            'summary': {}
        }
        
        # 1. 데이터 통계
        print("\n1️⃣ 데이터 통계 수집")
        benchmark_results['data_statistics'] = self.get_data_statistics(start_month, end_month)
        
        # 2. 이탈률 계산 벤치마크
        print("\n2️⃣ 이탈률 계산 벤치마크")
        months = self.analyzer._generate_month_range(start_month, end_month)
        
        churn_benchmarks = []
        for month in months[1:]:  # 첫 번째 월 제외
            benchmark = self.benchmark_churn_calculation(month, threshold)
            churn_benchmarks.append(benchmark)
        
        benchmark_results['benchmarks']['churn_calculation'] = churn_benchmarks
        
        # 3. 세그먼트 분석 벤치마크
        print("\n3️⃣ 세그먼트 분석 벤치마크")
        segment_types = ['gender', 'age_band', 'channel']
        
        segment_benchmarks = {}
        for segment_type in segment_types:
            benchmark = self.benchmark_segment_analysis(segment_type, start_month, end_month)
            segment_benchmarks[segment_type] = benchmark
        
        benchmark_results['benchmarks']['segment_analysis'] = segment_benchmarks
        
        # 4. 종합 결과 요약
        print("\n4️⃣ 종합 결과 요약")
        
        # 이탈률 계산 정확도
        churn_accuracy = sum(1 for b in churn_benchmarks if b['comparison']['is_valid']) / len(churn_benchmarks) * 100 if churn_benchmarks else 100
        
        # 세그먼트 분석 정확도
        segment_accuracy = sum(1 for b in segment_benchmarks.values() if b['comparison']['is_valid']) / len(segment_benchmarks) * 100 if segment_benchmarks else 100
        
        # 전체 성능
        total_analytics_time = sum(b['performance']['analytics_time'] for b in churn_benchmarks) + sum(b['performance']['analytics_time'] for b in segment_benchmarks.values())
        total_manual_time = sum(b['performance']['manual_time'] for b in churn_benchmarks) + sum(b['performance']['manual_time'] for b in segment_benchmarks.values())
        
        benchmark_results['summary'] = {
            'overall_accuracy': (churn_accuracy + segment_accuracy) / 2,
            'churn_calculation_accuracy': churn_accuracy,
            'segment_analysis_accuracy': segment_accuracy,
            'performance': {
                'total_analytics_time': total_analytics_time,
                'total_manual_time': total_manual_time,
                'overall_speed_ratio': total_manual_time / total_analytics_time if total_analytics_time > 0 else 0
            },
            'data_quality': benchmark_results['data_statistics']
        }
        
        # 결과 출력
        print("\n🎯 벤치마크 결과 요약")
        print("-" * 40)
        print(f"전체 정확도: {benchmark_results['summary']['overall_accuracy']:.1f}%")
        print(f"이탈률 계산 정확도: {churn_accuracy:.1f}%")
        print(f"세그먼트 분석 정확도: {segment_accuracy:.1f}%")
        print(f"전체 성능 비율: {benchmark_results['summary']['performance']['overall_speed_ratio']:.2f}x")
        
        # 리포트 저장
        report_filename = f"benchmark_report_{start_month}_{end_month}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(benchmark_results, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n📄 상세 벤치마크 리포트가 저장되었습니다: {report_filename}")
        
        return benchmark_results

def main():
    """메인 실행 함수"""
    
    print("실제 데이터 벤치마크 검증기")
    print("=" * 50)
    print("이 도구는 실제 운영 데이터를 사용하여 analytics.py의")
    print("계산 결과를 벤치마크하고 검증합니다.")
    print("\n사용 예시:")
    print("""
# 데이터베이스 연결 후 실행
from database import get_db
from benchmark_validation import BenchmarkValidator

db = next(get_db())
validator = BenchmarkValidator(db)

# 종합 벤치마크 실행
validator.run_comprehensive_benchmark('2024-01', '2024-03')

# 개별 벤치마크 실행
validator.benchmark_churn_calculation('2024-02')
validator.benchmark_segment_analysis('gender', '2024-01', '2024-02')
    """)

if __name__ == "__main__":
    main()
