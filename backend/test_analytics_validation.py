"""
Analytics.py의 계산식 검증을 위한 단위 테스트
이 파일은 이탈률, 유지율, 세그먼트 분석 등의 계산이 올바른지 검증합니다.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import Base, Event, User
from analytics import ChurnAnalyzer
import tempfile
import os

class TestAnalyticsCalculations:
    """Analytics 계산식 검증 테스트 클래스"""
    
    @pytest.fixture(scope="class")
    def setup_test_db(self):
        """테스트용 데이터베이스 설정"""
        # 임시 SQLite 데이터베이스 생성
        db_fd, db_path = tempfile.mkstemp()
        engine = create_engine(f'sqlite:///{db_path}', echo=False)
        
        # 테이블 생성
        Base.metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        session = Session()
        
        yield session, engine
        
        session.close()
        os.close(db_fd)
        os.unlink(db_path)
    
    @pytest.fixture
    def sample_data(self):
        """검증용 샘플 데이터 생성"""
        return {
            'users': [
                {'user_hash': 'user1', 'gender': 'M', 'age_band': '30s', 'channel': 'web'},
                {'user_hash': 'user2', 'gender': 'F', 'age_band': '20s', 'channel': 'app'},
                {'user_hash': 'user3', 'gender': 'M', 'age_band': '40s', 'channel': 'web'},
                {'user_hash': 'user4', 'gender': 'F', 'age_band': '30s', 'channel': 'app'},
                {'user_hash': 'user5', 'gender': 'M', 'age_band': '50s', 'channel': 'web'},
            ],
            'events': [
                # 2024-01월 데이터 (이전 월)
                {'user_hash': 'user1', 'created_at': '2024-01-15 10:00:00', 'action': 'login'},
                {'user_hash': 'user1', 'created_at': '2024-01-20 14:00:00', 'action': 'post'},
                {'user_hash': 'user2', 'created_at': '2024-01-10 09:00:00', 'action': 'login'},
                {'user_hash': 'user3', 'created_at': '2024-01-25 16:00:00', 'action': 'login'},
                {'user_hash': 'user4', 'created_at': '2024-01-12 11:00:00', 'action': 'login'},
                
                # 2024-02월 데이터 (현재 월)
                {'user_hash': 'user1', 'created_at': '2024-02-10 10:00:00', 'action': 'login'},  # 유지
                {'user_hash': 'user2', 'created_at': '2024-02-05 09:00:00', 'action': 'login'},  # 유지
                # user3은 2월에 활동 없음 (이탈)
                # user4는 2월에 활동 없음 (이탈)
                {'user_hash': 'user5', 'created_at': '2024-02-15 12:00:00', 'action': 'login'},  # 신규
            ]
        }
    
    def insert_test_data(self, session, sample_data):
        """테스트 데이터 삽입"""
        # 사용자 데이터 삽입
        for user_data in sample_data['users']:
            user = User(**user_data)
            session.add(user)
        
        # 이벤트 데이터 삽입
        for event_data in sample_data['events']:
            event = Event(**event_data)
            session.add(event)
        
        session.commit()
    
    def test_churn_rate_calculation(self, setup_test_db, sample_data):
        """이탈률 계산 검증"""
        session, engine = setup_test_db
        self.insert_test_data(session, sample_data)
        
        analyzer = ChurnAnalyzer(session)
        metrics = analyzer.get_monthly_metrics('2024-02', threshold=1)
        
        # 예상 결과 계산
        # 이전 월(2024-01) 활성 사용자: user1, user2, user3, user4 (4명)
        # 현재 월(2024-02) 활성 사용자: user1, user2, user5 (3명)
        # 이탈한 사용자: user3, user4 (2명)
        # 유지된 사용자: user1, user2 (2명)
        
        expected_churn_rate = (2 / 4) * 100  # 50%
        expected_retention_rate = (2 / 4) * 100  # 50%
        
        assert metrics['churn_rate'] == expected_churn_rate, f"이탈률 계산 오류: 예상 {expected_churn_rate}%, 실제 {metrics['churn_rate']}%"
        assert metrics['retention_rate'] == expected_retention_rate, f"유지율 계산 오류: 예상 {expected_retention_rate}%, 실제 {metrics['retention_rate']}%"
        assert metrics['churned_users'] == 2, f"이탈 사용자 수 오류: 예상 2명, 실제 {metrics['churned_users']}명"
        assert metrics['retained_users'] == 2, f"유지 사용자 수 오류: 예상 2명, 실제 {metrics['retained_users']}명"
    
    def test_segment_analysis_calculation(self, setup_test_db, sample_data):
        """세그먼트별 분석 계산 검증"""
        session, engine = setup_test_db
        self.insert_test_data(session, sample_data)
        
        analyzer = ChurnAnalyzer(session)
        segment_results = analyzer._analyze_segment('gender', '2024-01', '2024-02')
        
        # 성별별 분석 결과 검증
        gender_results = {result['segment_value']: result for result in segment_results}
        
        # 남성(M) 분석: 이전 월 3명(user1, user3, user5), 현재 월 2명(user1, user5), 이탈 1명(user3)
        # 여성(F) 분석: 이전 월 2명(user2, user4), 현재 월 1명(user2), 이탈 1명(user4)
        
        if 'M' in gender_results:
            male_churn_rate = gender_results['M']['churn_rate']
            expected_male_churn = (1 / 3) * 100  # 33.3%
            assert abs(male_churn_rate - expected_male_churn) < 0.1, f"남성 이탈률 계산 오류"
        
        if 'F' in gender_results:
            female_churn_rate = gender_results['F']['churn_rate']
            expected_female_churn = (1 / 2) * 100  # 50%
            assert abs(female_churn_rate - expected_female_churn) < 0.1, f"여성 이탈률 계산 오류"
    
    def test_threshold_filtering(self, setup_test_db):
        """활성 사용자 임계값 필터링 검증"""
        session, engine = setup_test_db
        
        # 임계값 테스트용 데이터 생성
        test_data = {
            'users': [
                {'user_hash': 'low_activity', 'gender': 'M', 'age_band': '30s', 'channel': 'web'},
                {'user_hash': 'high_activity', 'gender': 'F', 'age_band': '20s', 'channel': 'app'},
            ],
            'events': [
                # 낮은 활동 사용자 (1개 이벤트)
                {'user_hash': 'low_activity', 'created_at': '2024-01-15 10:00:00', 'action': 'login'},
                {'user_hash': 'low_activity', 'created_at': '2024-02-10 10:00:00', 'action': 'login'},
                
                # 높은 활동 사용자 (3개 이벤트)
                {'user_hash': 'high_activity', 'created_at': '2024-01-15 10:00:00', 'action': 'login'},
                {'user_hash': 'high_activity', 'created_at': '2024-01-20 14:00:00', 'action': 'post'},
                {'user_hash': 'high_activity', 'created_at': '2024-01-25 16:00:00', 'action': 'view'},
                {'user_hash': 'high_activity', 'created_at': '2024-02-10 10:00:00', 'action': 'login'},
            ]
        }
        
        self.insert_test_data(session, test_data)
        
        analyzer = ChurnAnalyzer(session)
        
        # 임계값 2로 테스트
        metrics_threshold_2 = analyzer.get_monthly_metrics('2024-02', threshold=2)
        
        # 임계값 2에서는 high_activity만 활성 사용자로 인정
        # 이전 월 활성: high_activity (1명)
        # 현재 월 활성: high_activity (1명)
        # 이탈: 0명
        # 유지: 1명
        
        assert metrics_threshold_2['churned_users'] == 0, "임계값 2에서 이탈 사용자가 있어서는 안됨"
        assert metrics_threshold_2['retained_users'] == 1, "임계값 2에서 유지 사용자는 1명이어야 함"
    
    def test_data_quality_calculation(self, setup_test_db, sample_data):
        """데이터 품질 계산 검증"""
        session, engine = setup_test_db
        self.insert_test_data(session, sample_data)
        
        analyzer = ChurnAnalyzer(session)
        quality = analyzer._check_data_quality('2024-01', '2024-02')
        
        # 데이터 품질 지표 검증
        total_events = len(sample_data['events'])
        assert quality['total_events'] == total_events, "총 이벤트 수가 일치하지 않음"
        assert quality['data_completeness'] == 100.0, "모든 이벤트가 유효해야 함"
        assert quality['unique_users'] == len(sample_data['users']), "고유 사용자 수가 일치하지 않음"
    
    def test_inactivity_calculation(self, setup_test_db):
        """장기 미접속 사용자 계산 검증"""
        session, engine = setup_test_db
        
        # 장기 미접속 테스트용 데이터
        test_data = {
            'users': [
                {'user_hash': 'active_user', 'gender': 'M', 'age_band': '30s', 'channel': 'web'},
                {'user_hash': 'inactive_user', 'gender': 'F', 'age_band': '20s', 'channel': 'app'},
            ],
            'events': [
                # 활성 사용자 (최근 활동)
                {'user_hash': 'active_user', 'created_at': '2024-02-15 10:00:00', 'action': 'login'},
                
                # 비활성 사용자 (90일 이전 활동)
                {'user_hash': 'inactive_user', 'created_at': '2024-01-01 10:00:00', 'action': 'login'},
            ]
        }
        
        self.insert_test_data(session, test_data)
        
        analyzer = ChurnAnalyzer(session)
        inactivity = analyzer._analyze_inactivity('2024-02', [90])
        
        # 90일 미접속 사용자는 inactive_user 1명이어야 함
        assert inactivity['inactive_90d'] == 1, "90일 미접속 사용자 수가 올바르지 않음"
    
    def test_reactivation_calculation(self, setup_test_db):
        """재활성 사용자 계산 검증"""
        session, engine = setup_test_db
        
        # 재활성 테스트용 데이터
        test_data = {
            'users': [
                {'user_hash': 'reactivated_user', 'gender': 'M', 'age_band': '30s', 'channel': 'web'},
                {'user_hash': 'regular_user', 'gender': 'F', 'age_band': '20s', 'channel': 'app'},
            ],
            'events': [
                # 재활성 사용자 (30일 이상 간격 후 재활성)
                {'user_hash': 'reactivated_user', 'created_at': '2024-01-01 10:00:00', 'action': 'login'},
                {'user_hash': 'reactivated_user', 'created_at': '2024-02-15 10:00:00', 'action': 'login'},
                
                # 정기 사용자 (지속적 활동)
                {'user_hash': 'regular_user', 'created_at': '2024-01-15 10:00:00', 'action': 'login'},
                {'user_hash': 'regular_user', 'created_at': '2024-02-10 10:00:00', 'action': 'login'},
            ]
        }
        
        self.insert_test_data(session, test_data)
        
        analyzer = ChurnAnalyzer(session)
        reactivation = analyzer._analyze_reactivation('2024-02', gap_days=30)
        
        # reactivated_user는 30일 이상 간격 후 재활성되었으므로 1명이어야 함
        assert reactivation['reactivated_users'] == 1, "재활성 사용자 수가 올바르지 않음"

if __name__ == "__main__":
    # 테스트 실행을 위한 간단한 스크립트
    print("Analytics 계산식 검증 테스트")
    print("=" * 50)
    print("테스트를 실행하려면 다음 명령어를 사용하세요:")
    print("pytest backend/test_analytics_validation.py -v")
    print("\n또는 개별 테스트 실행:")
    print("pytest backend/test_analytics_validation.py::TestAnalyticsCalculations::test_churn_rate_calculation -v")
