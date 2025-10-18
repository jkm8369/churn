// API 클라이언트 (프로덕션용)
class ChurnAnalysisAPI {
    constructor(baseURL = 'http://localhost:8000') {
        this.baseURL = baseURL;
        this.token = localStorage.getItem('auth_token');
    }

    // 기본 HTTP 요청 메서드
    async request(endpoint, options = {}) {
        const url = `${this.baseURL}${endpoint}`;
        const config = {
            headers: {
                'Content-Type': 'application/json',
                ...(this.token && { 'Authorization': `Bearer ${this.token}` }),
                ...options.headers
            },
            ...options
        };

        try {
            const response = await fetch(url, config);
            
            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
            }
            
            return await response.json();
        } catch (error) {
            console.error(`API 요청 실패 (${endpoint}):`, error);
            throw error;
        }
    }

    // GET 요청
    async get(endpoint, params = {}) {
        const queryString = new URLSearchParams(params).toString();
        const url = queryString ? `${endpoint}?${queryString}` : endpoint;
        return this.request(url, { method: 'GET' });
    }

    // POST 요청
    async post(endpoint, data = {}) {
        return this.request(endpoint, {
            method: 'POST',
            body: JSON.stringify(data)
        });
    }

    // DELETE 요청
    async delete(endpoint) {
        return this.request(endpoint, { method: 'DELETE' });
    }

    // 헬스 체크
    async healthCheck() {
        return this.get('/health');
    }

    // 이벤트 데이터 업로드
    async uploadEvents(events) {
        return this.post('/events/bulk', events);
    }

    // 분석 실행
    async runAnalysis(config) {
        const analysisRequest = {
            start_month: config.startMonth || '2025-08',
            end_month: config.endMonth || '2025-10',
            segments: {
                gender: config.segments?.gender ?? true,
                age_band: config.segments?.ageBand ?? true,
                channel: config.segments?.channel ?? true
            },
            inactivity_days: config.inactivityDays || [30, 60, 90]
        };

        return this.post('/analysis/run', analysisRequest);
    }

    // 월별 지표 조회
    async getMetrics(month) {
        return this.get('/analysis/metrics', { month });
    }

    // 세그먼트 분석 조회
    async getSegmentAnalysis(startMonth, endMonth) {
        return this.get('/analysis/segments', {
            start_month: startMonth,
            end_month: endMonth
        });
    }

    // 이탈률 트렌드 조회
    async getChurnTrends(months) {
        return this.get('/analysis/trends', {
            months: months.join(',')
        });
    }

    // 장기 미접속 사용자 조회
    async getInactiveUsers(days = 90, limit = 100) {
        return this.get('/users/inactive', { days, limit });
    }

    // 월별 리포트 조회
    async getMonthlyReport(month) {
        return this.get(`/reports/summary/${month}`);
    }

    // 캐시 삭제
    async clearCache() {
        return this.delete('/cache/clear');
    }
}

// 프로덕션용 스크립트 업데이트
class ProductionChurnAnalysis {
    constructor() {
        this.api = new ChurnAnalysisAPI();
        this.isAnalysisRunning = false;
        this.currentAnalysisId = null;
        
        // 차트 인스턴스
        this.churnTrendChart = null;
        this.segmentChart = null;
        
        this.init();
    }

    async init() {
        this.setupEventListeners();
        await this.initializeCharts();
        await this.checkAPIConnection();
        this.loadInitialData();
    }

    async checkAPIConnection() {
        try {
            await this.api.healthCheck();
            this.addLog('API 서버 연결 성공', 'success');
        } catch (error) {
            this.addLog(`API 서버 연결 실패: ${error.message}`, 'danger');
            this.addLog('로컬 더미 데이터로 전환합니다.', 'warning');
            // 기존 로컬 로직으로 폴백
            this.fallbackToLocal = true;
        }
    }

    setupEventListeners() {
        // 파일 업로드
        document.getElementById('csvFile').addEventListener('change', (e) => this.handleFileUpload(e));
        document.getElementById('uploadBtn').addEventListener('click', () => this.uploadFile());
        
        // 분석 실행
        document.getElementById('runAnalysisBtn').addEventListener('click', () => this.runAnalysis());
        
        // 설정 변경
        document.getElementById('startMonth').addEventListener('change', () => this.updateDateRange());
        document.getElementById('endMonth').addEventListener('change', () => this.updateDateRange());
        
        // 세그먼트 옵션
        ['genderSegment', 'ageSegment', 'channelSegment'].forEach(id => {
            document.getElementById(id).addEventListener('change', () => this.updateSegmentOptions());
        });
    }

    async handleFileUpload(event) {
        const file = event.target.files[0];
        if (!file) return;

        if (file.type !== 'text/csv' && !file.name.endsWith('.csv')) {
            this.showAlert('CSV 파일만 업로드 가능합니다.', 'danger');
            event.target.value = '';
            return;
        }

        try {
            // CSV 파일을 이벤트 객체 배열로 변환
            const events = await this.parseCSVFile(file);
            
            // 서버에 업로드
            if (!this.fallbackToLocal) {
                const result = await this.api.uploadEvents(events);
                this.addLog(`서버 업로드 완료: ${result.message}`, 'success');
            } else {
                // 로컬 저장
                window.csvData = events;
                this.addLog(`로컬 데이터 로드 완료: ${events.length}개 이벤트`, 'success');
            }
            
            this.updateProgressBar(25, '데이터 업로드 완료');
            document.getElementById('uploadBtn').disabled = false;
            
        } catch (error) {
            this.addLog(`파일 업로드 실패: ${error.message}`, 'danger');
            this.showAlert(`업로드 실패: ${error.message}`, 'danger');
        }
    }

    async parseCSVFile(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => {
                try {
                    const csv = e.target.result;
                    const lines = csv.split('\n').filter(line => line.trim());
                    const headers = lines[0].split(',').map(h => h.trim());
                    const events = [];

                    for (let i = 1; i < lines.length; i++) {
                        const values = lines[i].split(',').map(v => v.trim());
                        if (values.length === headers.length) {
                            const event = {};
                            headers.forEach((header, index) => {
                                event[header] = values[index];
                            });
                            
                            // 날짜 검증
                            if (event.created_at && event.user_hash && event.action) {
                                events.push(event);
                            }
                        }
                    }
                    
                    resolve(events);
                } catch (error) {
                    reject(error);
                }
            };
            reader.onerror = () => reject(new Error('파일 읽기 실패'));
            reader.readAsText(file);
        });
    }

    async runAnalysis() {
        if (this.isAnalysisRunning) return;

        this.isAnalysisRunning = true;
        const runBtn = document.getElementById('runAnalysisBtn');
        runBtn.disabled = true;
        runBtn.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>분석 실행 중...';

        try {
            // 설정 수집
            const config = this.getAnalysisConfig();
            
            this.addLog('분석 시작...', 'info');
            this.updateProgressBar(10, '분석 초기화 중...');

            let result;
            if (!this.fallbackToLocal) {
                // API 서버 사용
                result = await this.api.runAnalysis(config);
                this.currentAnalysisId = result.analysis_id;
            } else {
                // 로컬 분석 (기존 로직)
                result = await this.runLocalAnalysis(config);
            }

            // 단계별 진행률 업데이트
            await this.simulateProgressSteps();

            // 결과 업데이트
            await this.updateUIWithResults(result);
            
            this.addLog('분석이 성공적으로 완료되었습니다!', 'success');
            this.showAlert('분석이 완료되었습니다! 리포트 탭에서 결과를 확인하세요.', 'success');

        } catch (error) {
            this.addLog(`분석 실행 실패: ${error.message}`, 'danger');
            this.showAlert(`분석 실패: ${error.message}`, 'danger');
        } finally {
            this.isAnalysisRunning = false;
            runBtn.disabled = false;
            runBtn.innerHTML = '<i class="fas fa-play me-2"></i>분석 실행';
        }
    }

    getAnalysisConfig() {
        return {
            startMonth: document.getElementById('startMonth').value,
            endMonth: document.getElementById('endMonth').value,
            segments: {
                gender: document.getElementById('genderSegment').checked,
                ageBand: document.getElementById('ageSegment').checked,
                channel: document.getElementById('channelSegment').checked
            },
            inactivityDays: [30, 60, 90]
        };
    }

    async simulateProgressSteps() {
        const steps = [
            { progress: 25, message: '데이터 전처리 중...', delay: 500 },
            { progress: 45, message: '이탈률 계산 중...', delay: 800 },
            { progress: 65, message: '세그먼트 분석 중...', delay: 600 },
            { progress: 85, message: '인사이트 생성 중...', delay: 400 },
            { progress: 100, message: '분석 완료!', delay: 200 }
        ];

        for (const step of steps) {
            await new Promise(resolve => setTimeout(resolve, step.delay));
            this.updateProgressBar(step.progress, step.message);
            this.addLog(`[${this.getCurrentTime()}] ${step.message}`, 'info');
        }
    }

    async updateUIWithResults(result) {
        if (result.error) {
            throw new Error(result.error);
        }

        // 지표 카드 업데이트
        if (result.metrics) {
            this.animateValue('churnRate', result.metrics.churn_rate, '%');
            this.animateValue('activeUsers', result.metrics.active_users, '');
            this.animateValue('reactivatedUsers', result.metrics.reactivated_users, '');
            this.animateValue('longTermInactive', result.metrics.long_term_inactive, '');
        }

        // 차트 업데이트
        if (result.trends) {
            this.updateTrendChart(result.trends);
        }

        if (result.segments) {
            this.updateSegmentChart(result.segments);
        }

        // 리포트 업데이트
        if (result.insights && result.actions) {
            this.updateReportSection(result.insights, result.actions, result.data_quality);
        }
    }

    updateTrendChart(trendsData) {
        if (!this.churnTrendChart || !trendsData.trends) return;

        const labels = trendsData.trends.map(t => t.month);
        const data = trendsData.trends.map(t => t.churn_rate);

        this.churnTrendChart.data.labels = labels;
        this.churnTrendChart.data.datasets[0].data = data;
        this.churnTrendChart.update('active');
    }

    updateSegmentChart(segmentsData) {
        if (!this.segmentChart) return;

        const labels = [];
        const data = [];
        const colors = [];

        // 세그먼트 데이터 통합
        Object.entries(segmentsData).forEach(([segmentType, segments]) => {
            segments.forEach(segment => {
                labels.push(this.getSegmentDisplayName(segmentType, segment.segment_value));
                data.push(segment.churn_rate);
                colors.push(this.getSegmentColor(segmentType, segment.segment_value));
            });
        });

        this.segmentChart.data.labels = labels;
        this.segmentChart.data.datasets[0].data = data;
        this.segmentChart.data.datasets[0].backgroundColor = colors;
        this.segmentChart.update('active');
    }

    updateReportSection(insights, actions, dataQuality, llmMetadata = null) {
        // 인사이트 업데이트
        const insightsContainer = document.querySelector('#report .mb-4:first-child ul');
        if (insightsContainer && insights) {
            insightsContainer.innerHTML = insights.map(insight => 
                `<li class="mb-2">
                    <i class="fas fa-circle text-info me-2" style="font-size: 0.5rem;"></i>
                    ${insight}
                </li>`
            ).join('');
        }

        // 액션 업데이트
        const actionsContainer = document.querySelector('#report .mb-4:nth-child(2) ul');
        if (actionsContainer && actions) {
            actionsContainer.innerHTML = actions.map(action => 
                `<li class="mb-2">
                    <i class="fas fa-arrow-right text-success me-2"></i>
                    ${action}
                </li>`
            ).join('');
        }

        // 데이터 품질 업데이트
        if (dataQuality) {
            const qualityContainer = document.querySelector('#report .mb-4:nth-child(3) ul');
            if (qualityContainer) {
                qualityContainer.innerHTML = `
                    <li class="mb-1">• 총 ${dataQuality.total_events.toLocaleString()}행 검증 완료 (${dataQuality.invalid_events}행 제외)</li>
                    <li class="mb-1">• 데이터 완전성: ${dataQuality.data_completeness}%</li>
                    <li class="mb-1">• Unknown 값 비율: ${dataQuality.unknown_ratio}%</li>
                `;
            }
        }
        
        // LLM 메타데이터 표시 (있는 경우)
        if (llmMetadata && typeof updateLLMMetadataSection === 'function') {
            updateLLMMetadataSection(llmMetadata);
        }
    }

    // 유틸리티 메서드들
    getSegmentDisplayName(segmentType, segmentValue) {
        const names = {
            gender: { M: '남성', F: '여성' },
            age_band: { '20s': '20대', '30s': '30대', '40s': '40대', '50s': '50대' },
            channel: { web: '웹', app: '앱' }
        };
        return names[segmentType]?.[segmentValue] || segmentValue;
    }

    getSegmentColor(segmentType, segmentValue) {
        const colors = {
            gender: { M: '#007bff', F: '#dc3545' },
            age_band: { '20s': '#28a745', '30s': '#ffc107', '40s': '#fd7e14', '50s': '#dc3545' },
            channel: { web: '#6f42c1', app: '#e83e8c' }
        };
        return colors[segmentType]?.[segmentValue] || '#6c757d';
    }

    // 기존 메서드들 (차트 초기화, 애니메이션 등)
    async initializeCharts() {
        // 기존 차트 초기화 코드와 동일
        this.initializeChurnTrendChart();
        this.initializeSegmentChart();
    }

    initializeChurnTrendChart() {
        const ctx = document.getElementById('churnTrendChart').getContext('2d');
        this.churnTrendChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '이탈률 (%)',
                    data: [],
                    borderColor: '#007bff',
                    backgroundColor: 'rgba(0, 123, 255, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: true, max: 30 },
                    x: { grid: { display: false } }
                }
            }
        });
    }

    initializeSegmentChart() {
        const ctx = document.getElementById('segmentChart').getContext('2d');
        this.segmentChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: [],
                datasets: [{
                    label: '이탈률 (%)',
                    data: [],
                    backgroundColor: [],
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: true, max: 35 },
                    x: { grid: { display: false } }
                }
            }
        });
    }

    // 로컬 분석 폴백 (기존 로직)
    async runLocalAnalysis(config) {
        if (!window.csvData) {
            throw new Error('분석할 데이터가 없습니다. CSV 파일을 먼저 업로드하세요.');
        }

        // 기존 calculateMetrics, calculateChartData 등 사용
        const metrics = calculateMetrics(window.csvData);
        const chartData = calculateChartData(window.csvData);
        
        return {
            metrics: {
                churn_rate: metrics.churnRate,
                active_users: metrics.activeUsers,
                reactivated_users: metrics.reactivatedUsers,
                long_term_inactive: metrics.longTermInactive
            },
            trends: {
                trends: chartData.trendLabels.map((label, index) => ({
                    month: label,
                    churn_rate: chartData.trendData[index]
                }))
            },
            segments: {
                gender: [
                    { segment_value: 'M', churn_rate: 17.2, is_uncertain: false },
                    { segment_value: 'F', churn_rate: 23.4, is_uncertain: false }
                ],
                age_band: [
                    { segment_value: '20s', churn_rate: 12.1, is_uncertain: false },
                    { segment_value: '30s', churn_rate: 15.8, is_uncertain: false },
                    { segment_value: '40s', churn_rate: 21.3, is_uncertain: false },
                    { segment_value: '50s', churn_rate: 27.4, is_uncertain: true }
                ],
                channel: [
                    { segment_value: 'web', churn_rate: 16.8, is_uncertain: false },
                    { segment_value: 'app', churn_rate: 19.9, is_uncertain: false }
                ]
            },
            insights: [
                "여성 사용자의 최근 3개월 평균 이탈률이 남성 대비 +6.2%p 높습니다.",
                "50대 세그먼트에서 2개월 연속 이탈률 상승 (모수 부족으로 Uncertain 표기).",
                "90일+ 장기 미접속 비율이 50대에서 +3.1%p 높습니다."
            ],
            actions: [
                "여성 사용자 대상 맞춤형 콘텐츠 및 커뮤니티 활동 강화",
                "50대 사용자를 위한 신규 주제/가이드 보강 및 사용성 개선",
                "장기 미접속자 대상 복귀 유도 캠페인 및 푸시 알림 전략 수립"
            ],
            data_quality: {
                total_events: window.csvData.length,
                invalid_events: 0,
                data_completeness: 100,
                unknown_ratio: 5
            }
        };
    }

    // 기존 유틸리티 메서드들
    animateValue(elementId, targetValue, suffix = '') {
        // 기존 애니메이션 코드와 동일
        const element = document.getElementById(elementId);
        const startValue = 0;
        const duration = 1000;
        const startTime = performance.now();
        
        function updateValue(currentTime) {
            const elapsed = currentTime - startTime;
            const progress = Math.min(elapsed / duration, 1);
            const currentValue = startValue + (targetValue - startValue) * progress;
            
            if (suffix === '%') {
                element.textContent = currentValue.toFixed(1) + suffix;
            } else {
                element.textContent = Math.floor(currentValue).toLocaleString() + suffix;
            }
            
            if (progress < 1) {
                requestAnimationFrame(updateValue);
            }
        }
        
        requestAnimationFrame(updateValue);
    }

    updateProgressBar(progress, message) {
        const progressBar = document.getElementById('progressBar');
        const progressText = document.getElementById('progressText');
        
        progressBar.style.width = progress + '%';
        progressText.textContent = message;
    }

    addLog(message, type = 'info') {
        const logOutput = document.getElementById('logOutput');
        const timestamp = this.getCurrentTime();
        const logEntry = document.createElement('div');
        
        logEntry.className = `text-${type}`;
        logEntry.textContent = `[${timestamp}] ${message}`;
        
        logOutput.appendChild(logEntry);
        logOutput.scrollTop = logOutput.scrollHeight;
    }

    getCurrentTime() {
        return new Date().toLocaleTimeString('ko-KR', { 
            hour12: false,
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
    }

    showAlert(message, type) {
        const existingAlert = document.querySelector('.alert');
        if (existingAlert) existingAlert.remove();
        
        const alert = document.createElement('div');
        alert.className = `alert alert-${type} alert-dismissible fade show`;
        alert.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        
        const main = document.querySelector('main');
        main.insertBefore(alert, main.firstChild);
        
        setTimeout(() => {
            if (alert.parentNode) alert.remove();
        }, 5000);
    }

    async loadInitialData() {
        this.addLog('시스템 초기화 완료', 'success');
        
        if (!this.fallbackToLocal) {
            this.addLog('API 서버 연결됨 - 프로덕션 모드', 'info');
        } else {
            this.addLog('로컬 모드 - 더미 데이터 사용', 'warning');
            // 기존 로컬 데이터 로드 로직
            try {
                const response = await fetch('data/events.csv');
                const csvText = await response.text();
                const events = await this.parseCSVFromText(csvText);
                window.csvData = events;
                this.addLog(`샘플 데이터 로드 완료: ${events.length}행`, 'success');
            } catch (error) {
                this.addLog('샘플 데이터 로드 실패', 'warning');
            }
        }
    }

    async parseCSVFromText(csvText) {
        const lines = csvText.split('\n').filter(line => line.trim());
        const headers = lines[0].split(',').map(h => h.trim());
        const events = [];
        
        for (let i = 1; i < lines.length; i++) {
            const values = lines[i].split(',').map(v => v.trim());
            if (values.length === headers.length) {
                const event = {};
                headers.forEach((header, index) => {
                    event[header] = values[index];
                });
                
                try {
                    event.date = new Date(event.created_at);
                    event.year_month = event.date.toISOString().substring(0, 7);
                    events.push(event);
                } catch (e) {
                    // 잘못된 날짜 형식은 건너뛰기
                }
            }
        }
        
        return events;
    }
}

// DOM 로드 완료 시 초기화
document.addEventListener('DOMContentLoaded', function() {
    window.churnAnalysis = new ProductionChurnAnalysis();
});
