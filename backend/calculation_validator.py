"""
Analytics.py의 계산식을 수동으로 검증하기 위한 도구
이 도구를 사용하여 계산 결과를 직접 확인하고 검증할 수 있습니다.
"""

import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import Base, Event, User
from analytics import ChurnAnalyzer
import json

class CalculationValidator:
    """계산식 검증을 위한 도구 클래스"""
    
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
    
    def validate_churn_calculation(self, month: str, threshold: int = 1, verbose: bool = True):
        """이탈률 계산을 단계별로 검증"""
        
        print(f"🔍 이탈률 계산 검증 - {month}월 (임계값: {threshold})")
        print("=" * 60)
        
        # 1. 원시 데이터 조회
        query = text(f"""
        WITH monthly_users AS (
            SELECT 
                {self._get_month_trunc('created_at')} as month,
                user_hash,
                COUNT(*) as event_count
            FROM events 
            WHERE {self._get_month_trunc('created_at')} IN (
                :prev_month, :curr_month
            )
            GROUP BY {self._get_month_trunc('created_at')}, user_hash
            HAVING COUNT(*) >= :threshold
        )
        SELECT 
            month,
            user_hash,
            event_count
        FROM monthly_users
        ORDER BY month, user_hash
        """)
        
        current_month = month
        previous_month = self.analyzer._get_previous_month(month)
        
        results = self.db.execute(query, {
            "curr_month": f"{current_month}-01",
            "prev_month": f"{previous_month}-01",
            "threshold": threshold
        }).fetchall()
        
        if verbose:
            print(f"📊 원시 데이터 (임계값 {threshold} 이상):")
            print(f"이전 월: {previous_month}")
            print(f"현재 월: {current_month}")
            print("-" * 40)
            
            prev_users = []
            curr_users = []
            
            for row in results:
                month_str = row.month.strftime('%Y-%m')
                if month_str == previous_month:
                    prev_users.append(row.user_hash)
                elif month_str == current_month:
                    curr_users.append(row.user_hash)
                print(f"{month_str}: {row.user_hash} (이벤트 {row.event_count}개)")
            
            print(f"\n이전 월 활성 사용자: {len(prev_users)}명")
            print(f"사용자 목록: {prev_users}")
            print(f"\n현재 월 활성 사용자: {len(curr_users)}명")
            print(f"사용자 목록: {curr_users}")
        
        # 2. 이탈/유지 사용자 계산
        prev_users_set = set()
        curr_users_set = set()
        
        for row in results:
            month_str = row.month.strftime('%Y-%m')
            if month_str == previous_month:
                prev_users_set.add(row.user_hash)
            elif month_str == current_month:
                curr_users_set.add(row.user_hash)
        
        churned_users = prev_users_set - curr_users_set
        retained_users = prev_users_set & curr_users_set
        
        if verbose:
            print(f"\n📈 계산 결과:")
            print(f"이탈한 사용자: {len(churned_users)}명")
            print(f"이탈 사용자 목록: {list(churned_users)}")
            print(f"유지된 사용자: {len(retained_users)}명")
            print(f"유지 사용자 목록: {list(retained_users)}")
        
        # 3. 이탈률/유지율 계산
        prev_active = len(prev_users_set)
        churned = len(churned_users)
        retained = len(retained_users)
        
        churn_rate = (churned / prev_active * 100) if prev_active > 0 else 0
        retention_rate = (retained / prev_active * 100) if prev_active > 0 else 0
        
        if verbose:
            print(f"\n🧮 최종 계산:")
            print(f"이탈률 = {churned} / {prev_active} × 100 = {churn_rate:.1f}%")
            print(f"유지율 = {retained} / {prev_active} × 100 = {retention_rate:.1f}%")
        
        # 4. Analytics 클래스 결과와 비교
        analytics_result = self.analyzer.get_monthly_metrics(month, threshold)
        
        if verbose:
            print(f"\n✅ Analytics 클래스 결과와 비교:")
            print(f"이탈률: 계산값 {churn_rate:.1f}% vs Analytics {analytics_result.get('churn_rate', 0):.1f}%")
            print(f"유지율: 계산값 {retention_rate:.1f}% vs Analytics {analytics_result.get('retention_rate', 0):.1f}%")
            print(f"이탈 사용자: 계산값 {churned}명 vs Analytics {analytics_result.get('churned_users', 0)}명")
            print(f"유지 사용자: 계산값 {retained}명 vs Analytics {analytics_result.get('retained_users', 0)}명")
        
        # 검증 결과
        is_valid = (
            abs(churn_rate - analytics_result.get('churn_rate', 0)) < 0.1 and
            abs(retention_rate - analytics_result.get('retention_rate', 0)) < 0.1 and
            churned == analytics_result.get('churned_users', 0) and
            retained == analytics_result.get('retained_users', 0)
        )
        
        print(f"\n{'✅ 검증 성공' if is_valid else '❌ 검증 실패'}")
        
        return {
            'month': month,
            'threshold': threshold,
            'previous_active': prev_active,
            'current_active': len(curr_users_set),
            'churned_users': churned,
            'retained_users': retained,
            'churn_rate': churn_rate,
            'retention_rate': retention_rate,
            'is_valid': is_valid,
            'analytics_result': analytics_result
        }
    
    def validate_segment_calculation(self, segment_type: str, start_month: str, end_month: str, verbose: bool = True):
        """세그먼트별 계산을 검증"""
        
        print(f"🔍 세그먼트별 계산 검증 - {segment_type} ({start_month} ~ {end_month})")
        print("=" * 60)
        
        # 원시 데이터 조회
        query = text(f"""
        SELECT 
            {segment_type} AS segment_value,
            {self._get_month_trunc('created_at')} AS month,
            user_hash,
            COUNT(*) as event_count
        FROM events 
        WHERE {self._get_month_trunc('created_at')} BETWEEN :start_month AND :end_month
          AND {segment_type} IS NOT NULL 
          AND {segment_type} != 'Unknown'
        GROUP BY {segment_type}, {self._get_month_trunc('created_at')}, user_hash
        ORDER BY {segment_type}, month, user_hash
        """)
        
        results = self.db.execute(query, {
            "start_month": f"{start_month}-01",
            "end_month": f"{end_month}-01"
        }).fetchall()
        
        if verbose:
            print(f"📊 원시 데이터:")
            current_segment = None
            for row in results:
                if current_segment != row.segment_value:
                    if current_segment is not None:
                        print()
                    current_segment = row.segment_value
                    print(f"\n{segment_type}: {row.segment_value}")
                    print("-" * 30)
                print(f"  {row.month.strftime('%Y-%m')}: {row.user_hash} (이벤트 {row.event_count}개)")
        
        # 세그먼트별 집계
        segment_data = {}
        for row in results:
            segment = row.segment_value
            month = row.month.strftime('%Y-%m')
            user = row.user_hash
            
            if segment not in segment_data:
                segment_data[segment] = {}
            if month not in segment_data[segment]:
                segment_data[segment][month] = set()
            
            segment_data[segment][month].add(user)
        
        # 월별 전환 계산
        months = self.analyzer._generate_month_range(start_month, end_month)
        
        if verbose:
            print(f"\n📈 세그먼트별 계산 결과:")
        
        validation_results = {}
        
        for segment, month_data in segment_data.items():
            if verbose:
                print(f"\n{segment_type}: {segment}")
                print("-" * 30)
            
            total_prev_active = 0
            total_curr_active = 0
            total_churned = 0
            
            for i in range(1, len(months)):
                curr_month = months[i]
                prev_month = months[i-1]
                
                prev_users = month_data.get(prev_month, set())
                curr_users = month_data.get(curr_month, set())
                
                churned = prev_users - curr_users
                retained = prev_users & curr_users
                
                total_prev_active += len(prev_users)
                total_curr_active += len(curr_users)
                total_churned += len(churned)
                
                if verbose and len(prev_users) > 0:
                    churn_rate = len(churned) / len(prev_users) * 100
                    print(f"  {prev_month} → {curr_month}:")
                    print(f"    이전 활성: {len(prev_users)}명")
                    print(f"    현재 활성: {len(curr_users)}명")
                    print(f"    이탈: {len(churned)}명")
                    print(f"    이탈률: {churn_rate:.1f}%")
            
            # 전체 기간 집계
            if total_prev_active > 0:
                overall_churn_rate = total_churned / total_prev_active * 100
                
                if verbose:
                    print(f"\n  전체 기간 집계:")
                    print(f"    총 이전 활성: {total_prev_active}명")
                    print(f"    총 현재 활성: {total_curr_active}명")
                    print(f"    총 이탈: {total_churned}명")
                    print(f"    전체 이탈률: {overall_churn_rate:.1f}%")
                
                validation_results[segment] = {
                    'previous_active': total_prev_active,
                    'current_active': total_curr_active,
                    'churned': total_churned,
                    'churn_rate': overall_churn_rate
                }
        
        # Analytics 클래스 결과와 비교
        analytics_results = self.analyzer._analyze_segment(segment_type, start_month, end_month)
        
        if verbose:
            print(f"\n✅ Analytics 클래스 결과와 비교:")
        
        is_all_valid = True
        
        for analytics_result in analytics_results:
            segment = analytics_result['segment_value']
            validation = validation_results.get(segment, {})
            
            if validation:
                churn_diff = abs(validation['churn_rate'] - analytics_result['churn_rate'])
                is_valid = churn_diff < 0.1
                is_all_valid = is_all_valid and is_valid
                
                if verbose:
                    print(f"\n{segment_type}: {segment}")
                    print(f"  이탈률: 검증값 {validation['churn_rate']:.1f}% vs Analytics {analytics_result['churn_rate']:.1f}% ({'✅' if is_valid else '❌'})")
                    print(f"  이전 활성: 검증값 {validation['previous_active']}명 vs Analytics {analytics_result['previous_active']}명")
                    print(f"  이탈 사용자: 검증값 {validation['churned']}명 vs Analytics {analytics_result['churned_users']}명")
        
        print(f"\n{'✅ 전체 검증 성공' if is_all_valid else '❌ 검증 실패 항목 존재'}")
        
        return {
            'segment_type': segment_type,
            'start_month': start_month,
            'end_month': end_month,
            'validation_results': validation_results,
            'analytics_results': analytics_results,
            'is_valid': is_all_valid
        }
    
    def validate_inactivity_calculation(self, month: str, days_list: list = [30, 60, 90], verbose: bool = True):
        """장기 미접속 계산을 검증"""
        
        print(f"🔍 장기 미접속 계산 검증 - {month}월")
        print("=" * 60)
        
        month_end = f"{month}-01"
        cutoff_date = datetime.strptime(month_end, "%Y-%m-%d")
        
        validation_results = {}
        
        for days in days_list:
            specific_cutoff = cutoff_date - timedelta(days=days)
            
            # 원시 데이터 조회
            query = text("""
            SELECT user_hash, MAX(created_at) as last_activity
            FROM events
            GROUP BY user_hash
            ORDER BY last_activity DESC
            """)
            
            results = self.db.execute(query).fetchall()
            
            if verbose:
                print(f"\n📊 {days}일 미접속 기준 (기준일: {specific_cutoff.strftime('%Y-%m-%d')})")
                print("-" * 40)
            
            inactive_count = 0
            active_count = 0
            
            for row in results:
                is_inactive = row.last_activity < specific_cutoff
                if is_inactive:
                    inactive_count += 1
                else:
                    active_count += 1
                
                if verbose and inactive_count <= 10:  # 처음 10명만 표시
                    status = "미접속" if is_inactive else "활성"
                    print(f"  {row.user_hash}: {row.last_activity.strftime('%Y-%m-%d')} ({status})")
            
            if verbose:
                print(f"\n총 활성 사용자: {active_count}명")
                print(f"총 미접속 사용자: {inactive_count}명")
            
            validation_results[f'inactive_{days}d'] = inactive_count
        
        # Analytics 클래스 결과와 비교
        analytics_results = self.analyzer._analyze_inactivity(month, days_list)
        
        if verbose:
            print(f"\n✅ Analytics 클래스 결과와 비교:")
        
        is_all_valid = True
        
        for days in days_list:
            key = f'inactive_{days}d'
            validation_count = validation_results[key]
            analytics_count = analytics_results[key]
            is_valid = validation_count == analytics_count
            is_all_valid = is_all_valid and is_valid
            
            if verbose:
                print(f"  {days}일 미접속: 검증값 {validation_count}명 vs Analytics {analytics_count}명 ({'✅' if is_valid else '❌'})")
        
        print(f"\n{'✅ 전체 검증 성공' if is_all_valid else '❌ 검증 실패 항목 존재'}")
        
        return {
            'month': month,
            'validation_results': validation_results,
            'analytics_results': analytics_results,
            'is_valid': is_all_valid
        }
    
    def generate_verification_report(self, month: str, threshold: int = 1):
        """전체 검증 리포트 생성"""
        
        print("🔍 Analytics 계산식 전체 검증 리포트")
        print("=" * 80)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'month': month,
            'threshold': threshold,
            'validations': {}
        }
        
        # 1. 이탈률 계산 검증
        print("\n1️⃣ 이탈률 계산 검증")
        churn_validation = self.validate_churn_calculation(month, threshold, verbose=False)
        report['validations']['churn_rate'] = churn_validation
        print(f"   결과: {'✅ 성공' if churn_validation['is_valid'] else '❌ 실패'}")
        
        # 2. 세그먼트별 계산 검증
        print("\n2️⃣ 세그먼트별 계산 검증")
        segment_types = ['gender', 'age_band', 'channel']
        segment_validations = {}
        
        for segment_type in segment_types:
            print(f"   {segment_type} 검증 중...")
            validation = self.validate_segment_calculation(segment_type, month, month, verbose=False)
            segment_validations[segment_type] = validation
            print(f"   {segment_type}: {'✅ 성공' if validation['is_valid'] else '❌ 실패'}")
        
        report['validations']['segments'] = segment_validations
        
        # 3. 장기 미접속 계산 검증
        print("\n3️⃣ 장기 미접속 계산 검증")
        inactivity_validation = self.validate_inactivity_calculation(month, verbose=False)
        report['validations']['inactivity'] = inactivity_validation
        print(f"   결과: {'✅ 성공' if inactivity_validation['is_valid'] else '❌ 실패'}")
        
        # 4. 전체 검증 결과
        all_valid = (
            churn_validation['is_valid'] and
            all(v['is_valid'] for v in segment_validations.values()) and
            inactivity_validation['is_valid']
        )
        
        print(f"\n🎯 전체 검증 결과: {'✅ 모든 계산식이 올바릅니다' if all_valid else '❌ 일부 계산식에 문제가 있습니다'}")
        
        # 리포트 저장
        report_filename = f"verification_report_{month}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"\n📄 상세 리포트가 저장되었습니다: {report_filename}")
        
        return report

def main():
    """메인 실행 함수 - 실제 사용 예시"""
    
    print("Analytics 계산식 검증 도구")
    print("=" * 50)
    print("이 도구를 사용하려면 데이터베이스 연결이 필요합니다.")
    print("\n사용 예시:")
    print("""
# 데이터베이스 연결
from database import get_db
from calculation_validator import CalculationValidator

db = next(get_db())
validator = CalculationValidator(db)

# 개별 검증
validator.validate_churn_calculation('2024-02', threshold=1)
validator.validate_segment_calculation('gender', '2024-01', '2024-02')
validator.validate_inactivity_calculation('2024-02')

# 전체 검증 리포트
validator.generate_verification_report('2024-02')
    """)

if __name__ == "__main__":
    main()
