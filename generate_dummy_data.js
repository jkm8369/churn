// 더미 데이터 생성 스크립트
function generateDummyData() {
    const users = [];
    const events = [];
    
    // 사용자 프로필 생성
    const genders = ['M', 'F', 'Unknown'];
    const ageBands = ['10s', '20s', '30s', '40s', '50s', '60s', 'Unknown'];
    const channels = ['web', 'app', 'Unknown'];
    const actions = ['post', 'comment'];
    
    // 1000명의 사용자 생성
    for (let i = 1; i <= 1000; i++) {
        users.push({
            user_hash: `u${String(i).padStart(4, '0')}`,
            gender: getRandomWeighted(genders, [45, 50, 5]), // 남성 45%, 여성 50%, Unknown 5%
            age_band: getRandomWeighted(ageBands, [5, 25, 30, 25, 10, 3, 2]), // 20-30대 중심
            channel: getRandomWeighted(channels, [40, 55, 5]) // 앱 사용자가 더 많음
        });
    }
    
    // 2025-07부터 2025-10까지 이벤트 생성
    const startDate = new Date('2025-07-01');
    const endDate = new Date('2025-10-31');
    
    users.forEach(user => {
        // 각 사용자의 활동 패턴 결정
        const activityLevel = Math.random();
        let monthlyActivity = {};
        
        if (activityLevel < 0.1) {
            // 10% - 매우 활발한 사용자 (매월 활동)
            monthlyActivity = { '2025-07': 0.9, '2025-08': 0.9, '2025-09': 0.9, '2025-10': 0.9 };
        } else if (activityLevel < 0.3) {
            // 20% - 활발한 사용자
            monthlyActivity = { '2025-07': 0.8, '2025-08': 0.7, '2025-09': 0.6, '2025-10': 0.5 };
        } else if (activityLevel < 0.6) {
            // 30% - 보통 사용자
            monthlyActivity = { '2025-07': 0.6, '2025-08': 0.5, '2025-09': 0.3, '2025-10': 0.2 };
        } else if (activityLevel < 0.8) {
            // 20% - 가끔 사용자
            monthlyActivity = { '2025-07': 0.4, '2025-08': 0.3, '2025-09': 0.1, '2025-10': 0.05 };
        } else {
            // 20% - 이탈 사용자 (초기에만 활동)
            monthlyActivity = { '2025-07': 0.7, '2025-08': 0.2, '2025-09': 0.0, '2025-10': 0.0 };
        }
        
        // 성별/연령별 이탈 패턴 적용
        if (user.gender === 'F') {
            // 여성 사용자는 이탈률이 약간 높음
            Object.keys(monthlyActivity).forEach(month => {
                monthlyActivity[month] *= 0.9;
            });
        }
        
        if (user.age_band === '50s' || user.age_band === '60s') {
            // 50대 이상은 이탈률이 높음
            Object.keys(monthlyActivity).forEach(month => {
                monthlyActivity[month] *= 0.8;
            });
        }
        
        if (user.age_band === '20s') {
            // 20대는 활동률이 높음
            Object.keys(monthlyActivity).forEach(month => {
                monthlyActivity[month] *= 1.2;
                if (monthlyActivity[month] > 1) monthlyActivity[month] = 1;
            });
        }
        
        // 각 월별로 이벤트 생성
        Object.keys(monthlyActivity).forEach(yearMonth => {
            if (Math.random() < monthlyActivity[yearMonth]) {
                const monthStart = new Date(yearMonth + '-01');
                const monthEnd = new Date(yearMonth + '-' + getDaysInMonth(yearMonth));
                
                // 해당 월에 1-5개의 이벤트 생성
                const eventCount = Math.floor(Math.random() * 5) + 1;
                
                for (let i = 0; i < eventCount; i++) {
                    const eventDate = new Date(
                        monthStart.getTime() + 
                        Math.random() * (monthEnd.getTime() - monthStart.getTime())
                    );
                    
                    events.push({
                        user_hash: user.user_hash,
                        created_at: eventDate.toISOString(),
                        action: getRandomWeighted(actions, [60, 40]), // post 60%, comment 40%
                        gender: user.gender,
                        age_band: user.age_band,
                        channel: user.channel
                    });
                }
            }
        });
    });
    
    // 이벤트를 날짜순으로 정렬
    events.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
    
    return events;
}

// 가중치 기반 랜덤 선택
function getRandomWeighted(items, weights) {
    const totalWeight = weights.reduce((sum, weight) => sum + weight, 0);
    let random = Math.random() * totalWeight;
    
    for (let i = 0; i < items.length; i++) {
        random -= weights[i];
        if (random <= 0) {
            return items[i];
        }
    }
    
    return items[items.length - 1];
}

// 해당 월의 일수 반환
function getDaysInMonth(yearMonth) {
    const [year, month] = yearMonth.split('-');
    return new Date(year, month, 0).getDate();
}

// CSV 형태로 변환
function eventsToCSV(events) {
    const headers = ['user_hash', 'created_at', 'action', 'gender', 'age_band', 'channel'];
    const csvLines = [headers.join(',')];
    
    events.forEach(event => {
        const line = headers.map(header => event[header]).join(',');
        csvLines.push(line);
    });
    
    return csvLines.join('\n');
}

// 실행
if (typeof module !== 'undefined' && module.exports) {
    // Node.js 환경
    module.exports = { generateDummyData, eventsToCSV };
} else {
    // 브라우저 환경
    console.log('더미 데이터 생성 함수가 로드되었습니다.');
    console.log('generateDummyData()를 실행하여 데이터를 생성하세요.');
}
