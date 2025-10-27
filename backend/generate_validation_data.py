"""
검증용 샘플 데이터 생성 스크립트
이 스크립트는 analytics.py의 계산식을 검증하기 위한 다양한 시나리오의 테스트 데이터를 생성합니다.
"""

from datetime import datetime, timedelta
from sqlalchemy import text
from models import Event, User

class ValidationDataGenerator:
    """검증용 데이터 생성기"""
    
    def __init__(self, db_session):
        self.db = db_session
        self.engine = db_session.bind
        
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
    
    def clear_existing_data(self):
        """기존 데이터 삭제"""
        print("🗑️ 기존 데이터 삭제 중...")
        
        # 외래키 제약 조건 비활성화 (SQLite)
        if 'sqlite' in str(self.engine.url):
            self.db.execute(text("PRAGMA foreign_keys=OFF"))
        
        # 테이블 데이터 삭제
        self.db.execute(text("DELETE FROM events"))
        self.db.execute(text("DELETE FROM users"))
        self.db.execute(text("DELETE FROM monthly_metrics"))
        self.db.execute(text("DELETE FROM user_segments"))
        
        self.db.commit()
        
        print("✅ 기존 데이터 삭제 완료")
    
    def generate_basic_scenario(self):
        """기본 시나리오: 간단한 이탈률 계산 검증용"""
        
        print("📊 기본 시나리오 데이터 생성 중...")
        
        # 사용자 데이터 생성
        users_data = [
            {'user_hash': 'user_001', 'gender': 'M', 'age_band': '30s', 'channel': 'web'},
            {'user_hash': 'user_002', 'gender': 'F', 'age_band': '20s', 'channel': 'app'},
            {'user_hash': 'user_003', 'gender': 'M', 'age_band': '40s', 'channel': 'web'},
            {'user_hash': 'user_004', 'gender': 'F', 'age_band': '30s', 'channel': 'app'},
            {'user_hash': 'user_005', 'gender': 'M', 'age_band': '50s', 'channel': 'web'},
        ]
        
        for user_data in users_data:
            user = User(**user_data)
            self.db.add(user)
        
        # 이벤트 데이터 생성
        events_data = [
            # 2024-01월 (이전 월)
            {'user_hash': 'user_001', 'created_at': '2024-01-15 10:00:00', 'action': 'login'},
            {'user_hash': 'user_001', 'created_at': '2024-01-20 14:00:00', 'action': 'post'},
            {'user_hash': 'user_002', 'created_at': '2024-01-10 09:00:00', 'action': 'login'},
            {'user_hash': 'user_003', 'created_at': '2024-01-25 16:00:00', 'action': 'login'},
            {'user_hash': 'user_004', 'created_at': '2024-01-12 11:00:00', 'action': 'login'},
            
            # 2024-02월 (현재 월)
            {'user_hash': 'user_001', 'created_at': '2024-02-10 10:00:00', 'action': 'login'},  # 유지
            {'user_hash': 'user_002', 'created_at': '2024-02-05 09:00:00', 'action': 'login'},  # 유지
            # user_003은 2월에 활동 없음 (이탈)
            # user_004는 2월에 활동 없음 (이탈)
            {'user_hash': 'user_005', 'created_at': '2024-02-15 12:00:00', 'action': 'login'},  # 신규
        ]
        
        for event_data in events_data:
            event = Event(**event_data)
            self.db.add(event)
        
        self.db.commit()
        
        print("✅ 기본 시나리오 생성 완료")
        print("   - 이전 월 활성 사용자: 4명 (user_001, user_002, user_003, user_004)")
        print("   - 현재 월 활성 사용자: 3명 (user_001, user_002, user_005)")
        print("   - 예상 이탈률: 50% (2명 이탈 / 4명)")
        print("   - 예상 유지율: 50% (2명 유지 / 4명)")
    
    def generate_threshold_scenario(self):
        """임계값 시나리오: 활성 사용자 임계값 테스트용"""
        
        print("📊 임계값 시나리오 데이터 생성 중...")
        
        # 사용자 데이터 추가
        users_data = [
            {'user_hash': 'low_activity_001', 'gender': 'M', 'age_band': '30s', 'channel': 'web'},
            {'user_hash': 'low_activity_002', 'gender': 'F', 'age_band': '20s', 'channel': 'app'},
            {'user_hash': 'high_activity_001', 'gender': 'M', 'age_band': '40s', 'channel': 'web'},
            {'user_hash': 'high_activity_002', 'gender': 'F', 'age_band': '30s', 'channel': 'app'},
        ]
        
        for user_data in users_data:
            user = User(**user_data)
            self.db.add(user)
        
        # 이벤트 데이터 생성
        events_data = [
            # 낮은 활동 사용자들 (1개 이벤트)
            {'user_hash': 'low_activity_001', 'created_at': '2024-01-15 10:00:00', 'action': 'login'},
            {'user_hash': 'low_activity_001', 'created_at': '2024-02-10 10:00:00', 'action': 'login'},
            {'user_hash': 'low_activity_002', 'created_at': '2024-01-20 14:00:00', 'action': 'login'},
            {'user_hash': 'low_activity_002', 'created_at': '2024-02-05 09:00:00', 'action': 'login'},
            
            # 높은 활동 사용자들 (3개 이상 이벤트)
            {'user_hash': 'high_activity_001', 'created_at': '2024-01-15 10:00:00', 'action': 'login'},
            {'user_hash': 'high_activity_001', 'created_at': '2024-01-20 14:00:00', 'action': 'post'},
            {'user_hash': 'high_activity_001', 'created_at': '2024-01-25 16:00:00', 'action': 'view'},
            {'user_hash': 'high_activity_001', 'created_at': '2024-02-10 10:00:00', 'action': 'login'},
            
            {'user_hash': 'high_activity_002', 'created_at': '2024-01-12 11:00:00', 'action': 'login'},
            {'user_hash': 'high_activity_002', 'created_at': '2024-01-18 13:00:00', 'action': 'post'},
            {'user_hash': 'high_activity_002', 'created_at': '2024-01-22 15:00:00', 'action': 'view'},
            {'user_hash': 'high_activity_002', 'created_at': '2024-02-05 09:00:00', 'action': 'login'},
        ]
        
        for event_data in events_data:
            event = Event(**event_data)
            self.db.add(event)
        
        self.db.commit()
        
        print("✅ 임계값 시나리오 생성 완료")
        print("   - 임계값 1: 모든 사용자 활성 (4명)")
        print("   - 임계값 2: 높은 활동 사용자만 활성 (2명)")
        print("   - 임계값 3: 높은 활동 사용자만 활성 (2명)")
    
    def generate_segment_scenario(self):
        """세그먼트 시나리오: 세그먼트별 분석 테스트용"""
        
        print("📊 세그먼트 시나리오 데이터 생성 중...")
        
        # 다양한 세그먼트의 사용자 데이터
        users_data = [
            # 남성 사용자들
            {'user_hash': 'male_young_web', 'gender': 'M', 'age_band': '20s', 'channel': 'web'},
            {'user_hash': 'male_young_app', 'gender': 'M', 'age_band': '20s', 'channel': 'app'},
            {'user_hash': 'male_middle_web', 'gender': 'M', 'age_band': '30s', 'channel': 'web'},
            {'user_hash': 'male_old_app', 'gender': 'M', 'age_band': '50s', 'channel': 'app'},
            
            # 여성 사용자들
            {'user_hash': 'female_young_web', 'gender': 'F', 'age_band': '20s', 'channel': 'web'},
            {'user_hash': 'female_young_app', 'gender': 'F', 'age_band': '20s', 'channel': 'app'},
            {'user_hash': 'female_middle_web', 'gender': 'F', 'age_band': '30s', 'channel': 'web'},
            {'user_hash': 'female_old_app', 'gender': 'F', 'age_band': '50s', 'channel': 'app'},
        ]
        
        for user_data in users_data:
            user = User(**user_data)
            self.db.add(user)
        
        # 이벤트 데이터 생성 (세그먼트별로 다른 이탈 패턴)
        events_data = [
            # 남성 사용자들 - 높은 이탈률
            {'user_hash': 'male_young_web', 'created_at': '2024-01-15 10:00:00', 'action': 'login'},
            {'user_hash': 'male_young_web', 'created_at': '2024-01-20 14:00:00', 'action': 'post'},
            {'user_hash': 'male_young_app', 'created_at': '2024-01-10 09:00:00', 'action': 'login'},
            {'user_hash': 'male_middle_web', 'created_at': '2024-01-25 16:00:00', 'action': 'login'},
            # 남성 사용자들은 2월에 활동 없음 (이탈)
            
            # 여성 사용자들 - 낮은 이탈률
            {'user_hash': 'female_young_web', 'created_at': '2024-01-12 11:00:00', 'action': 'login'},
            {'user_hash': 'female_young_app', 'created_at': '2024-01-18 13:00:00', 'action': 'login'},
            {'user_hash': 'female_middle_web', 'created_at': '2024-01-22 15:00:00', 'action': 'login'},
            {'user_hash': 'female_old_app', 'created_at': '2024-01-28 17:00:00', 'action': 'login'},
            
            # 2월 데이터 - 여성 사용자들만 유지
            {'user_hash': 'female_young_web', 'created_at': '2024-02-10 10:00:00', 'action': 'login'},
            {'user_hash': 'female_young_app', 'created_at': '2024-02-05 09:00:00', 'action': 'login'},
            {'user_hash': 'female_middle_web', 'created_at': '2024-02-15 12:00:00', 'action': 'login'},
            {'user_hash': 'female_old_app', 'created_at': '2024-02-20 14:00:00', 'action': 'login'},
        ]
        
        for event_data in events_data:
            event = Event(**event_data)
            self.db.add(event)
        
        self.db.commit()
        
        print("✅ 세그먼트 시나리오 생성 완료")
        print("   - 남성 사용자: 4명 모두 이탈 (100% 이탈률)")
        print("   - 여성 사용자: 4명 모두 유지 (0% 이탈률)")
        print("   - 성별별 이탈률 차이: 100%p")
    
    def generate_inactivity_scenario(self):
        """장기 미접속 시나리오: 미접속 분석 테스트용"""
        
        print("📊 장기 미접속 시나리오 데이터 생성 중...")
        
        # 사용자 데이터 추가
        users_data = [
            {'user_hash': 'very_active', 'gender': 'M', 'age_band': '30s', 'channel': 'web'},
            {'user_hash': 'recently_active', 'gender': 'F', 'age_band': '20s', 'channel': 'app'},
            {'user_hash': 'moderately_inactive', 'gender': 'M', 'age_band': '40s', 'channel': 'web'},
            {'user_hash': 'very_inactive', 'gender': 'F', 'age_band': '30s', 'channel': 'app'},
            {'user_hash': 'extremely_inactive', 'gender': 'M', 'age_band': '50s', 'channel': 'web'},
        ]
        
        for user_data in users_data:
            user = User(**user_data)
            self.db.add(user)
        
        # 이벤트 데이터 생성 (다양한 마지막 활동 시점)
        base_date = datetime(2024, 2, 1)
        
        events_data = [
            # 매우 활성 사용자 (최근 활동)
            {'user_hash': 'very_active', 'created_at': (base_date - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'), 'action': 'login'},
            
            # 최근 활동 사용자 (30일 이내)
            {'user_hash': 'recently_active', 'created_at': (base_date - timedelta(days=15)).strftime('%Y-%m-%d %H:%M:%S'), 'action': 'login'},
            
            # 중간 정도 비활성 사용자 (60일 이내, 30일 초과)
            {'user_hash': 'moderately_inactive', 'created_at': (base_date - timedelta(days=45)).strftime('%Y-%m-%d %H:%M:%S'), 'action': 'login'},
            
            # 매우 비활성 사용자 (90일 이내, 60일 초과)
            {'user_hash': 'very_inactive', 'created_at': (base_date - timedelta(days=75)).strftime('%Y-%m-%d %H:%M:%S'), 'action': 'login'},
            
            # 극도로 비활성 사용자 (90일 초과)
            {'user_hash': 'extremely_inactive', 'created_at': (base_date - timedelta(days=120)).strftime('%Y-%m-%d %H:%M:%S'), 'action': 'login'},
        ]
        
        for event_data in events_data:
            event = Event(**event_data)
            self.db.add(event)
        
        self.db.commit()
        
        print("✅ 장기 미접속 시나리오 생성 완료")
        print("   - 30일 미접속: 3명 (moderately_inactive, very_inactive, extremely_inactive)")
        print("   - 60일 미접속: 2명 (very_inactive, extremely_inactive)")
        print("   - 90일 미접속: 1명 (extremely_inactive)")
    
    def generate_reactivation_scenario(self):
        """재활성 시나리오: 재활성 사용자 분석 테스트용"""
        
        print("📊 재활성 시나리오 데이터 생성 중...")
        
        # 사용자 데이터 추가
        users_data = [
            {'user_hash': 'reactivated_user', 'gender': 'M', 'age_band': '30s', 'channel': 'web'},
            {'user_hash': 'regular_user', 'gender': 'F', 'age_band': '20s', 'channel': 'app'},
            {'user_hash': 'new_user', 'gender': 'M', 'age_band': '40s', 'channel': 'web'},
        ]
        
        for user_data in users_data:
            user = User(**user_data)
            self.db.add(user)
        
        # 이벤트 데이터 생성
        base_date = datetime(2024, 2, 1)
        
        events_data = [
            # 재활성 사용자 (30일 이상 간격 후 재활성)
            {'user_hash': 'reactivated_user', 'created_at': (base_date - timedelta(days=60)).strftime('%Y-%m-%d %H:%M:%S'), 'action': 'login'},
            {'user_hash': 'reactivated_user', 'created_at': (base_date + timedelta(days=15)).strftime('%Y-%m-%d %H:%M:%S'), 'action': 'login'},
            
            # 정기 사용자 (지속적 활동)
            {'user_hash': 'regular_user', 'created_at': (base_date - timedelta(days=15)).strftime('%Y-%m-%d %H:%M:%S'), 'action': 'login'},
            {'user_hash': 'regular_user', 'created_at': (base_date + timedelta(days=10)).strftime('%Y-%m-%d %H:%M:%S'), 'action': 'login'},
            
            # 신규 사용자 (2월에만 활동)
            {'user_hash': 'new_user', 'created_at': (base_date + timedelta(days=5)).strftime('%Y-%m-%d %H:%M:%S'), 'action': 'login'},
        ]
        
        for event_data in events_data:
            event = Event(**event_data)
            self.db.add(event)
        
        self.db.commit()
        
        print("✅ 재활성 시나리오 생성 완료")
        print("   - 재활성 사용자: 1명 (reactivated_user)")
        print("   - 정기 사용자: 1명 (regular_user)")
        print("   - 신규 사용자: 1명 (new_user)")
    
    def generate_comprehensive_scenario(self):
        """종합 시나리오: 모든 기능을 테스트할 수 있는 복합 데이터"""
        
        print("📊 종합 시나리오 데이터 생성 중...")
        
        # 더 많은 사용자와 복잡한 패턴
        users_data = []
        events_data = []
        
        # 다양한 조합의 사용자 생성
        genders = ['M', 'F']
        age_bands = ['20s', '30s', '40s', '50s']
        channels = ['web', 'app']
        
        user_counter = 1
        
        for gender in genders:
            for age_band in age_bands:
                for channel in channels:
                    user_hash = f'comprehensive_{gender}_{age_band}_{channel}_{user_counter:03d}'
                    users_data.append({
                        'user_hash': user_hash,
                        'gender': gender,
                        'age_band': age_band,
                        'channel': channel
                    })
                    user_counter += 1
        
        for user_data in users_data:
            user = User(**user_data)
            self.db.add(user)
        
        # 복잡한 이벤트 패턴 생성
        base_date = datetime(2024, 1, 1)
        
        for i, user_data in enumerate(users_data):
            user_hash = user_data['user_hash']
            
            # 각 사용자별로 다른 활동 패턴
            activity_level = i % 4  # 0: 매우 활성, 1: 활성, 2: 비활성, 3: 매우 비활성
            
            if activity_level == 0:  # 매우 활성 (모든 월에 활동)
                for month in range(1, 4):  # 1월, 2월, 3월
                    event_date = base_date.replace(month=month, day=15)
                    events_data.append({
                        'user_hash': user_hash,
                        'created_at': event_date.strftime('%Y-%m-%d %H:%M:%S'),
                        'action': 'login'
                    })
                    events_data.append({
                        'user_hash': user_hash,
                        'created_at': (event_date + timedelta(days=10)).strftime('%Y-%m-%d %H:%M:%S'),
                        'action': 'post'
                    })
            
            elif activity_level == 1:  # 활성 (1월, 2월만 활동)
                for month in range(1, 3):  # 1월, 2월
                    event_date = base_date.replace(month=month, day=20)
                    events_data.append({
                        'user_hash': user_hash,
                        'created_at': event_date.strftime('%Y-%m-%d %H:%M:%S'),
                        'action': 'login'
                    })
            
            elif activity_level == 2:  # 비활성 (1월만 활동)
                event_date = base_date.replace(month=1, day=25)
                events_data.append({
                    'user_hash': user_hash,
                    'created_at': event_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'action': 'login'
                })
            
            else:  # 매우 비활성 (1월 초에만 활동, 이후 장기 미접속)
                event_date = base_date.replace(month=1, day=5)
                events_data.append({
                    'user_hash': user_hash,
                    'created_at': event_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'action': 'login'
                })
        
        for event_data in events_data:
            event = Event(**event_data)
            self.db.add(event)
        
        self.db.commit()
        
        print("✅ 종합 시나리오 생성 완료")
        print(f"   - 총 사용자: {len(users_data)}명")
        print(f"   - 총 이벤트: {len(events_data)}개")
        print("   - 다양한 활동 패턴과 세그먼트 조합")
    
    def generate_all_scenarios(self):
        """모든 시나리오 생성"""
        
        print("🚀 모든 검증 시나리오 데이터 생성 시작")
        print("=" * 60)
        
        self.clear_existing_data()
        
        scenarios = [
            ("기본 시나리오", self.generate_basic_scenario),
            ("임계값 시나리오", self.generate_threshold_scenario),
            ("세그먼트 시나리오", self.generate_segment_scenario),
            ("장기 미접속 시나리오", self.generate_inactivity_scenario),
            ("재활성 시나리오", self.generate_reactivation_scenario),
            ("종합 시나리오", self.generate_comprehensive_scenario),
        ]
        
        for scenario_name, scenario_func in scenarios:
            print(f"\n📋 {scenario_name} 생성 중...")
            scenario_func()
        
        print("\n🎉 모든 시나리오 생성 완료!")
        print("=" * 60)
        
        # 데이터 요약 출력
        self.print_data_summary()
    
    def print_data_summary(self):
        """생성된 데이터 요약 출력"""
        
        print("\n📊 생성된 데이터 요약")
        print("-" * 40)
        
        # 사용자 수
        user_count = self.db.execute(text("SELECT COUNT(*) FROM users")).scalar()
        print(f"총 사용자 수: {user_count}명")
        
        # 이벤트 수
        event_count = self.db.execute(text("SELECT COUNT(*) FROM events")).scalar()
        print(f"총 이벤트 수: {event_count}개")
        
        # 월별 이벤트 수
        monthly_events = self.db.execute(text(f"""
            SELECT {self._get_month_trunc('created_at')} as month, COUNT(*) as count
            FROM events
            GROUP BY {self._get_month_trunc('created_at')}
            ORDER BY month
        """)).fetchall()
        
        print("\n월별 이벤트 수:")
        for row in monthly_events:
            month_str = row.month if isinstance(row.month, str) else row.month.strftime('%Y-%m')
            print(f"  {month_str}: {row.count}개")
        
        # 세그먼트별 사용자 수
        print("\n세그먼트별 사용자 수:")
        
        for segment in ['gender', 'age_band', 'channel']:
            segment_counts = self.db.execute(text(f"""
                SELECT {segment}, COUNT(*) as count
                FROM users
                GROUP BY {segment}
                ORDER BY count DESC
            """)).fetchall()
            
            print(f"  {segment}:")
            for row in segment_counts:
                print(f"    {row[0]}: {row[1]}명")

def main():
    """메인 실행 함수"""
    
    print("검증용 샘플 데이터 생성기")
    print("=" * 50)
    print("이 스크립트는 analytics.py의 계산식을 검증하기 위한")
    print("다양한 시나리오의 테스트 데이터를 생성합니다.")
    print("\n사용 예시:")
    print("""
# 데이터베이스 연결 후 실행
from database import get_db
from generate_validation_data import ValidationDataGenerator

db = next(get_db())
generator = ValidationDataGenerator(db)

# 모든 시나리오 생성
generator.generate_all_scenarios()

# 또는 개별 시나리오 생성
generator.generate_basic_scenario()
generator.generate_segment_scenario()
    """)

if __name__ == "__main__":
    main()
