// 전역 변수
// 차트 변수 제거됨
let isAnalysisRunning = false;

// DOM 로드 완료 시 초기화
document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM 로드 완료, 초기화 시작...');
    
    // 약간의 지연을 두고 초기화 (DOM 완전 로드 보장)
    setTimeout(() => {
        try {
            // 차트 초기화는 분석 후에만 수행
            setupEventListeners();
            updateStatusCards(); // 초기 상태 카드 설정
            
            // 초기 로그 추가
            addLog('시스템 초기화 완료', 'success');
            addLog('CSV 파일을 업로드하여 분석을 시작하세요', 'info');
            
            console.log('초기화 완료 (차트는 분석 후 표시)!');
        } catch (error) {
            console.error('초기화 중 오류 발생:', error);
            addLog('초기화 중 오류 발생: ' + error.message, 'danger');
        }
    }, 100);
});

// 이벤트 리스너 설정
function setupEventListeners() {
    console.log('setupEventListeners 함수 시작...');
    
    // 안전한 이벤트 리스너 추가 함수
    function safeAddEventListener(elementId, event, handler) {
        console.log(`요소 찾는 중: ${elementId}`);
        
        // 핸들러 함수 존재 확인
        if (typeof handler !== 'function') {
            console.error(`❌ 핸들러가 함수가 아닙니다: ${elementId} -> ${typeof handler}`);
            return;
        }
        
        const element = document.getElementById(elementId);
        if (element) {
            element.addEventListener(event, handler);
            console.log(`✅ 이벤트 리스너 추가됨: ${elementId}`);
        } else {
            console.warn(`❌ 요소를 찾을 수 없습니다: ${elementId}`);
        }
    }
    
    // 파일 업로드
    safeAddEventListener('csvFile', 'change', handleFileUpload);
    safeAddEventListener('uploadBtn', 'click', uploadFile);
    
    // 분석 실행
    safeAddEventListener('runAnalysisBtn', 'click', runAnalysis);
    
    // 설정 변경 감지
    safeAddEventListener('startDate', 'change', updateDateRange);
    safeAddEventListener('endDate', 'change', updateDateRange);
    
    // 세그먼트 체크박스
    safeAddEventListener('genderSegment', 'change', updateSegmentOptions);
    safeAddEventListener('ageSegment', 'change', updateSegmentOptions);
    safeAddEventListener('channelSegment', 'change', updateSegmentOptions);

}

// 차트 초기화 (제거됨)
function initializeCharts() {
    // 차트 기능이 제거되었습니다
    console.log('차트 기능이 제거되었습니다.');
}

// 월별 이탈률 트렌드 차트 초기화
function initializeChurnTrendChart() {
    const canvas = document.getElementById('churnTrendChart');
    if (!canvas) {
        console.error('[ERROR] churnTrendChart 캔버스를 찾을 수 없습니다');
        return;
    }
    const ctx = canvas.getContext('2d');
    
    churnTrendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: ['데이터 없음'],
            datasets: [{
                label: '이탈률 (%)',
                data: [0],
                borderColor: '#007bff',
                backgroundColor: 'rgba(0, 123, 255, 0.1)',
                borderWidth: 3,
                fill: true,
                tension: 0.4,
                pointBackgroundColor: '#007bff',
                pointBorderColor: '#ffffff',
                pointBorderWidth: 2,
                pointRadius: 6,
                pointHoverRadius: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    titleColor: '#ffffff',
                    bodyColor: '#ffffff',
                    borderColor: '#007bff',
                    borderWidth: 1,
                    callbacks: {
                        label: function(context) {
                            return `이탈률: ${context.parsed.y}%`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 25,
                    ticks: {
                        callback: function(value) {
                            return value + '%';
                        }
                    },
                    grid: {
                        color: 'rgba(0, 0, 0, 0.1)'
                    }
                },
                x: {
                    grid: {
                        display: false
                    }
                }
            },
            elements: {
                point: {
                    hoverBackgroundColor: '#dc3545'
                }
            }
        }
    });
    
    console.log('[DEBUG] churnTrendChart 초기화 완료:', !!churnTrendChart);
}

// 세그먼트별 이탈률 차트 초기화
function initializeSegmentChart() {
    const canvas = document.getElementById('segmentChart');
    if (!canvas) {
        console.error('[ERROR] segmentChart 캔버스를 찾을 수 없습니다');
        return;
    }
    const ctx = canvas.getContext('2d');
    
    segmentChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['세그먼트 선택 안됨'],
            datasets: [{
                label: '이탈률 (%)',
                data: [0],
                backgroundColor: ['#6c757d'],
                borderColor: ['#495057'],
                borderWidth: 1,
                borderRadius: 4,
                borderSkipped: false
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    titleColor: '#ffffff',
                    bodyColor: '#ffffff',
                    borderColor: '#007bff',
                    borderWidth: 1,
                    callbacks: {
                        label: function(context) {
                            const uncertain = context.dataIndex === 5 ? ' (Uncertain)' : '';
                            return `이탈률: ${context.parsed.y}%${uncertain}`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    // max 제거하여 동적 범위 사용
                    ticks: {
                        callback: function(value) {
                            return value + '%';
                        }
                    },
                    grid: {
                        color: 'rgba(0, 0, 0, 0.1)'
                    }
                },
                x: {
                    grid: {
                        display: false
                    }
                }
            }
        }
    });
    
    console.log('[DEBUG] segmentChart 초기화 완료:', !!segmentChart);
}

// 파일 업로드 처리
function handleFileUpload(event) {
    const file = event.target.files[0];
    if (file) {
        if (file.type !== 'text/csv' && !file.name.endsWith('.csv')) {
            showAlert('CSV 파일만 업로드 가능합니다.', 'danger');
            event.target.value = '';
            return;
        }
        
        if (file.size > 50 * 1024 * 1024) { // 50MB 제한
            showAlert('파일 크기는 50MB를 초과할 수 없습니다.', 'danger');
            event.target.value = '';
            return;
        }
        
        addLog(`파일 선택됨: ${file.name} (${formatFileSize(file.size)})`, 'info');
        document.getElementById('uploadBtn').disabled = false;
        
        // 파일 선택 시에는 데이터 상태만 부분 업데이트
        updateDataStatusForSelectedFile(file);
    }
}

// 파일 업로드 실행
function uploadFile() {
    const fileInput = document.getElementById('csvFile');
    const file = fileInput.files[0];
    
    if (!file) {
        showAlert('업로드할 파일을 선택해주세요.', 'warning');
        return;
    }
    
    const uploadBtn = document.getElementById('uploadBtn');
    uploadBtn.disabled = true;
    uploadBtn.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>업로드 중...';
    
    // 업로드 진행 상태 표시
    updateDataStatusForUploading(file);
    
    // 파일 읽기 시뮬레이션
    setTimeout(() => {
        // CSV 파일 검증 시뮬레이션
        validateCSVFile(file).then(result => {
            if (result.success) {
                addLog(`파일 업로드 완료: ${result.rows}행 검증됨 (${result.dropped}행 제외)`, 'success');
                showAlert('파일이 성공적으로 업로드되었습니다.', 'success');
                updateProgressBar(25, '데이터 업로드 완료');
                
                // 상태 카드 즉시 업데이트
                updateStatusCards();
                addLog('대시보드 상태 업데이트 완료', 'info');
            } else {
                addLog(`파일 업로드 실패: ${result.error}`, 'danger');
                showAlert(`업로드 실패: ${result.error}`, 'danger');
                
                // 실패 시 상태 원복
                updateDataStatusForSelectedFile(file);
            }
            
            uploadBtn.disabled = false;
            uploadBtn.innerHTML = '<i class="fas fa-cloud-upload-alt me-1"></i>업로드 실행';
        });
    }, 2000);
}

// CSV 파일 검증 및 파싱
function validateCSVFile(file) {
    return new Promise((resolve) => {
        const reader = new FileReader();
        reader.onload = function(e) {
            const csv = e.target.result;
            const lines = csv.split('\n').filter(line => line.trim());
            
            if (lines.length < 2) {
                resolve({ success: false, error: '데이터가 없습니다.' });
                return;
            }
            
            const headers = lines[0].split(',').map(h => h.trim());
            const requiredColumns = ['user_hash', 'created_at', 'action', 'gender', 'age_band', 'channel'];
            const missingColumns = requiredColumns.filter(col => !headers.includes(col));
            
            if (missingColumns.length > 0) {
                resolve({ 
                    success: false, 
                    error: `필수 컬럼 누락: ${missingColumns.join(', ')}` 
                });
                return;
            }
            
            // 실제 데이터 파싱
            const data = [];
            let droppedRows = 0;
            
            for (let i = 1; i < lines.length; i++) {
                const values = lines[i].split(',').map(v => v.trim());
                if (values.length !== headers.length) {
                    droppedRows++;
                    continue;
                }
                
                const row = {};
                headers.forEach((header, index) => {
                    row[header] = values[index];
                });
                
                // 데이터 검증
                if (!row.user_hash || !row.created_at || !row.action) {
                    droppedRows++;
                    continue;
                }
                
                // action 검증
                if (!['post', 'comment'].includes(row.action)) {
                    droppedRows++;
                    continue;
                }
                
                // 날짜 파싱
                try {
                    row.date = new Date(row.created_at);
                    row.year_month = row.date.toISOString().substring(0, 7); // YYYY-MM
                } catch (e) {
                    droppedRows++;
                    continue;
                }
                
                // Unknown 값 보정
                row.gender = row.gender || 'Unknown';
                row.age_band = row.age_band || 'Unknown';
                row.channel = row.channel || 'Unknown';
                
                data.push(row);
            }
            
            // 전역 변수에 저장
            window.csvData = data;
            
            resolve({
                success: true,
                rows: data.length,
                dropped: droppedRows,
                data: data
            });
        };
        reader.readAsText(file);
    });
}

// 분석 실행
function runAnalysis() {
    if (isAnalysisRunning) return;
    
    const fileInput = document.getElementById('csvFile');
    if (!fileInput.files[0]) {
        showAlert('먼저 CSV 파일을 업로드해주세요.', 'warning');
        return;
    }
    
    isAnalysisRunning = true;
    const runBtn = document.getElementById('runAnalysisBtn');
    runBtn.disabled = true;
    runBtn.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>분석 실행 중...';
    
    // 분석 상태 업데이트
    updateAnalysisStatus('running');
    
    addLog('분석 시작...', 'info');
    updateProgressBar(0, '분석 초기화 중...');
    
    // 분석 단계별 실행 시뮬레이션
    const analysisSteps = [
        { progress: 10, message: '데이터 전처리 중...', delay: 1000 },
        { progress: 25, message: '월별 활성 사용자 집계 중...', delay: 1500 },
        { progress: 40, message: '이탈률 계산 중...', delay: 1200 },
        { progress: 55, message: '세그먼트 분석 중...', delay: 1800 },
        { progress: 70, message: '장기 미접속 사용자 분석 중...', delay: 1000 },
        { progress: 85, message: '차트 생성 중...', delay: 800 },
        { progress: 95, message: '리포트 생성 중...', delay: 1000 },
        { progress: 100, message: '분석 완료!', delay: 500 }
    ];
    
    executeAnalysisSteps(analysisSteps, 0);
}

// 분석 단계별 실행
async function executeAnalysisSteps(steps, currentStep) {
    if (currentStep >= steps.length) {
        completeAnalysis();
        return;
    }
    
    const step = steps[currentStep];
    
    setTimeout(async () => {
        updateProgressBar(step.progress, step.message);
        addLog(`[${getCurrentTime()}] ${step.message}`, 'info');
        
        // 특정 단계에서 데이터 업데이트
        if (step.progress === 40) {
            await updateMetricCards();
        } else if (step.progress === 85) {
            await updateReportWithDynamicData();
        }
        
        executeAnalysisSteps(steps, currentStep + 1);
    }, step.delay);
}

// 분석 완료
function completeAnalysis() {
    isAnalysisRunning = false;
    const runBtn = document.getElementById('runAnalysisBtn');
    runBtn.disabled = false;
    runBtn.innerHTML = '<i class="fas fa-play me-2"></i>분석 실행';
    
    // 분석 상태 업데이트
    updateAnalysisStatus('completed');
    
    addLog('분석이 성공적으로 완료되었습니다!', 'success');
    
    showAlert('분석이 완료되었습니다! 리포트 탭에서 결과를 확인하세요.', 'success');
    
    // 리포트 탭으로 자동 전환
    setTimeout(() => {
        const reportTab = document.querySelector('a[href="#report"]');
        const tab = new bootstrap.Tab(reportTab);
        tab.show();
    }, 2000);
}

// 지표 카드 업데이트 (백엔드 API 사용)
async function updateMetricCards() {
    if (!window.csvData || window.csvData.length === 0) {
        // 메트릭 카드 숨기기
        document.getElementById('metricsRow').style.display = 'none';
        addLog('데이터가 없어서 지표를 표시하지 않습니다.', 'warning');
        return;
    }
    
    try {
        // 백엔드 API 호출로 통일된 계산 결과 사용
        const config = getCurrentConfig();
        console.log('[DEBUG] updateMetricCards - 현재 설정:', config);
        
        const backendResponse = await callBackendAPI(config);
        console.log('[DEBUG] updateMetricCards - 백엔드 응답:', backendResponse);
        
        // 백엔드 응답에서 메트릭과 세그먼트 데이터 분리
        const metrics = {
            churn_rate: backendResponse.churn_rate,
            active_users: backendResponse.active_users,
            reactivated_users: backendResponse.reactivated_users,
            long_term_inactive: backendResponse.long_term_inactive,
            previous_active_users: backendResponse.previous_active_users
        };
        
        // 세그먼트 분석 결과를 전역 변수에 저장 (인사이트 생성 시 사용)
        if (backendResponse.segments) {
            window.currentSegmentAnalysis = backendResponse.segments;
            console.log('[DEBUG] 세그먼트 분석 결과 저장:', window.currentSegmentAnalysis);
        }
        
        // 백엔드 응답이 모두 0이거나 오류인 경우 프론트엔드 계산 사용
        const isBackendDataEmpty = metrics.error || 
            (metrics.churn_rate === 0 && metrics.active_users === 0 && 
             metrics.reactivated_users === 0 && metrics.long_term_inactive === 0);
        
        if (isBackendDataEmpty) {
            if (metrics.error) {
                addLog(`백엔드 계산 실패: ${metrics.error}`, 'error');
            } else {
                addLog('백엔드에서 빈 데이터 반환, 로컬 계산 사용', 'warning');
            }
            // 폴백으로 프론트엔드 계산 사용
            const fallbackMetrics = calculateMetrics(window.csvData, config);
            console.log('[DEBUG] 폴백 메트릭 사용:', fallbackMetrics);
            updateMetricCardsWithData(fallbackMetrics, config);
            return;
        }
        
        // 메트릭 카드 표시
        document.getElementById('metricsRow').style.display = 'flex';
        
        // 백엔드에서 받은 데이터로 업데이트
        updateMetricCardsWithData(metrics, config);
        
        addLog('백엔드 API를 통한 정확한 계산 완료', 'success');
        
    } catch (error) {
        addLog(`API 호출 실패: ${error.message}`, 'warning');
        
        // 폴백으로 프론트엔드 계산 사용
        const config = getCurrentConfig();
        console.log('[DEBUG] 폴백 계산 - 설정:', config);
        const metrics = calculateMetrics(window.csvData, config);
        console.log('[DEBUG] 폴백 계산 - 결과:', metrics);
        updateMetricCardsWithData(metrics, config);
        
        addLog('로컬 계산으로 폴백 실행', 'info');
    }
}

// 메트릭 카드 데이터 업데이트 (공통 함수)
function updateMetricCardsWithData(metrics, config) {
    // 메트릭 카드 표시
    document.getElementById('metricsRow').style.display = 'flex';
    
    // 실제 계산된 값으로 업데이트
    animateValue('churnRate', metrics.churn_rate || metrics.churnRate || 0, '%');
    animateValue('activeUsers', metrics.active_users || metrics.activeUsers || 0, '');
    animateValue('reactivatedUsers', metrics.reactivated_users || metrics.reactivatedUsers || 0, '');
    animateValue('longTermInactive', metrics.long_term_inactive || metrics.longTermInactive || 0, '');
    
    // 새로운 동적 지표들 업데이트 (백엔드 형식에 맞춰 조정)
    const normalizedMetrics = {
        churnRate: metrics.churn_rate || metrics.churnRate || 0,
        activeUsers: metrics.active_users || metrics.activeUsers || 0,
        previousActiveUsers: metrics.previous_active_users || metrics.previousActiveUsers || 0,
        reactivatedUsers: metrics.reactivated_users || metrics.reactivatedUsers || 0,
        longTermInactive: metrics.long_term_inactive || metrics.longTermInactive || 0
    };
    
    updateAdvancedMetrics(normalizedMetrics, config);
}

// 백엔드 API 호출
async function callBackendAPI(config) {
    // 날짜를 월 형식으로 변환 (백엔드는 아직 월 단위)
    const startMonth = config.startDate ? config.startDate.substring(0, 7) : '2025-08';
    const endMonth = config.endDate ? config.endDate.substring(0, 7) : '2025-10';

    const inactivityThresholds = [30, 60, 90];
    
    const requestData = {
        start_month: startMonth,
        end_month: endMonth,
        segments: {
            gender: config.segments?.gender ?? false,
            age_band: config.segments?.ageBand ?? false,
            channel: config.segments?.channel ?? false
        },
        inactivity_days: inactivityThresholds,
        threshold: 1  // 최소 이벤트 수
    };
    
    console.log('[DEBUG] callBackendAPI - 요청 데이터:', requestData);
    
    const response = await fetch('http://localhost:8000/analysis/run', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestData)
    });
    
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    
    const result = await response.json();
    return result.metrics || result;
}

// 상태 카드 업데이트
function updateStatusCards() {
    // 데이터 상태 업데이트
    updateDataStatus();
    
    // 기간 상태 업데이트
    updatePeriodStatus();

    
    // 분석 상태 업데이트
    updateAnalysisStatus();
}

// 데이터 상태 업데이트
function updateDataStatus() {
    const dataStatus = document.getElementById('dataStatus');
    const dataInfo = document.getElementById('dataInfo');
    
    if (window.csvData && window.csvData.length > 0) {
        dataStatus.textContent = '로드됨';
        dataStatus.className = 'card-title text-success';
        dataInfo.textContent = `${window.csvData.length.toLocaleString()}행 데이터`;
        
        // 아이콘 색상 변경 (스피너에서 파일 아이콘으로 복원)
        const icon = dataStatus.parentElement.querySelector('i[class*="fa-"]');
        if (icon) icon.className = 'fas fa-file-csv fa-2x text-success mb-2';
    } else {
        dataStatus.textContent = '파일 없음';
        dataStatus.className = 'card-title text-secondary';
        dataInfo.textContent = 'CSV 파일을 업로드하세요';
        
        // 아이콘 색상 원복
        const icon = dataStatus.parentElement.querySelector('i[class*="fa-"]');
        if (icon) icon.className = 'fas fa-file-csv fa-2x text-secondary mb-2';
    }
}

// 파일 선택 시 데이터 상태 부분 업데이트
function updateDataStatusForSelectedFile(file) {
    const dataStatus = document.getElementById('dataStatus');
    const dataInfo = document.getElementById('dataInfo');
    
    if (file) {
        dataStatus.textContent = '선택됨';
        dataStatus.className = 'card-title text-warning';
        dataInfo.textContent = `${file.name} (${formatFileSize(file.size)})`;
        
        // 아이콘 색상 변경
        const icon = dataStatus.parentElement.querySelector('i[class*="fa-"]');
        if (icon) icon.className = 'fas fa-file-csv fa-2x text-warning mb-2';
    }
}

// 업로드 진행 중 상태 표시
function updateDataStatusForUploading(file) {
    const dataStatus = document.getElementById('dataStatus');
    const dataInfo = document.getElementById('dataInfo');
    
    if (file) {
        dataStatus.textContent = '처리중';
        dataStatus.className = 'card-title text-info';
        dataInfo.textContent = `${file.name} 파싱 중...`;
        
        // 아이콘 색상 변경 (스피너 효과)
        const icon = dataStatus.parentElement.querySelector('i[class*="fa-"]');
        if (icon) icon.className = 'fas fa-spinner fa-spin fa-2x text-info mb-2';
    }
}





// 기간 상태 업데이트
function updatePeriodStatus() {
    const periodStatus = document.getElementById('periodStatus');
    const periodInfo = document.getElementById('periodInfo');
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    
    if (startDate && endDate) {
        const start = new Date(startDate);
        const end = new Date(endDate);
        const diffDays = Math.ceil((end - start) / (1000 * 60 * 60 * 24));
        
        periodStatus.textContent = '설정됨';
        periodStatus.className = 'card-title text-success';
        periodInfo.textContent = `${diffDays}일 기간`;
        
        // 아이콘 색상 변경
        const icon = periodStatus.parentElement.querySelector('i[class*="fa-"]');
        if (icon) icon.className = 'fas fa-calendar-alt fa-2x text-success mb-2';
    } else {
        periodStatus.textContent = '미설정';
        periodStatus.className = 'card-title text-secondary';
        periodInfo.textContent = '날짜를 선택하세요';
        
        // 아이콘 색상 원복
        const icon = periodStatus.parentElement.querySelector('i[class*="fa-"]');
        if (icon) icon.className = 'fas fa-calendar-alt fa-2x text-info mb-2';
    }
}

// 분석 상태 업데이트
function updateAnalysisStatus(status = 'waiting') {
    const analysisStatus = document.getElementById('analysisStatus');
    const analysisInfo = document.getElementById('analysisInfo');
    const icon = analysisStatus.parentElement.querySelector('i[class*="fa-"]');
    
    switch (status) {
        case 'running':
            analysisStatus.textContent = '실행중';
            analysisStatus.className = 'card-title text-warning';
            analysisInfo.textContent = '분석 진행중...';
            if (icon) icon.className = 'fas fa-chart-line fa-2x text-warning mb-2';
            break;
        case 'completed':
            analysisStatus.textContent = '완료';
            analysisStatus.className = 'card-title text-success';
            analysisInfo.textContent = '분석이 완료되었습니다';
            if (icon) icon.className = 'fas fa-chart-line fa-2x text-success mb-2';
            break;
        case 'error':
            analysisStatus.textContent = '오류';
            analysisStatus.className = 'card-title text-danger';
            analysisInfo.textContent = '분석 중 오류 발생';
            if (icon) icon.className = 'fas fa-chart-line fa-2x text-danger mb-2';
            break;
        default: // waiting
            analysisStatus.textContent = '대기중';
            analysisStatus.className = 'card-title text-secondary';
            analysisInfo.textContent = '분석 실행을 클릭하세요';
            if (icon) icon.className = 'fas fa-chart-line fa-2x text-secondary mb-2';
    }
}

// 고급 지표 업데이트
function updateAdvancedMetrics(metrics, config) {
    // 1. 이탈률 심각도 및 이탈자 수
    const churnRate = metrics.churnRate || 0;
    const churnedCount = Math.round((metrics.previousActiveUsers || 0) * churnRate / 100);
    
    updateElement('churnedCount', churnedCount);
    updateChurnSeverity(churnRate);
    
    // 2. 활성 사용자 트렌드 및 증감률
    const growthRate = calculateGrowthRate(metrics.activeUsers, metrics.previousActiveUsers);
    updateElement('userGrowthRate', `${growthRate > 0 ? '+' : ''}${growthRate}%`);
    updateUserTrend(growthRate);
    
    // 3. 재활성률 및 복귀 패턴
    const reactivationRate = calculateReactivationRate(metrics.reactivatedUsers, metrics.longTermInactive);
    updateElement('reactivationRate', `${reactivationRate}%`);
    updateReturnPattern(reactivationRate);
    
    // 4. 위험도 레벨 및 손실 위험
    const riskPercentage = calculateRiskPercentage(metrics.longTermInactive, metrics.activeUsers);
    updateElement('lossRisk', `${riskPercentage}%`);
    updateRiskLevel(riskPercentage);
}

// 요소 업데이트 헬퍼 함수
function updateElement(elementId, value) {
    const element = document.getElementById(elementId);
    if (element) {
        element.textContent = value;
    }
}

// 이탈률 심각도 업데이트
function updateChurnSeverity(churnRate) {
    const element = document.getElementById('churnSeverity');
    if (!element) return;
    
    if (churnRate >= 20) {
        element.textContent = '매우높음';
        element.className = 'badge bg-danger';
    } else if (churnRate >= 15) {
        element.textContent = '높음';
        element.className = 'badge bg-warning text-dark';
    } else if (churnRate >= 10) {
        element.textContent = '보통';
        element.className = 'badge bg-primary';
    } else {
        element.textContent = '낮음';
        element.className = 'badge bg-success';
    }
}

// 사용자 트렌드 업데이트
function updateUserTrend(growthRate) {
    const element = document.getElementById('userTrend');
    if (!element) return;
    
    if (growthRate > 5) {
        element.textContent = '급성장';
        element.className = 'badge bg-success';
    } else if (growthRate > 0) {
        element.textContent = '성장';
        element.className = 'badge bg-primary';
    } else if (growthRate > -5) {
        element.textContent = '안정';
        element.className = 'badge bg-secondary';
    } else {
        element.textContent = '감소';
        element.className = 'badge bg-warning text-dark';
    }
}

// 복귀 패턴 업데이트
function updateReturnPattern(reactivationRate) {
    const element = document.getElementById('returnPattern');
    if (!element) return;
    
    if (reactivationRate > 5) {
        element.textContent = '매우좋음';
    } else if (reactivationRate > 3) {
        element.textContent = '좋음';
    } else if (reactivationRate > 1) {
        element.textContent = '보통';
    } else {
        element.textContent = '나쁨';
    }
}

// 위험도 레벨 업데이트
function updateRiskLevel(riskPercentage) {
    const element = document.getElementById('riskLevel');
    if (!element) return;
    
    if (riskPercentage > 15) {
        element.textContent = '고위험';
        element.className = 'badge bg-danger';
    } else if (riskPercentage > 10) {
        element.textContent = '중위험';
        element.className = 'badge bg-warning text-dark';
    } else {
        element.textContent = '저위험';
        element.className = 'badge bg-success';
    }
}

// 계산 헬퍼 함수들
function calculateGrowthRate(current, previous) {
    if (!previous || previous === 0) return 0;
    return Math.round(((current - previous) / previous) * 100 * 10) / 10;
}

function calculateReactivationRate(reactivated, longTermInactive) {
    const total = reactivated + longTermInactive;
    if (total === 0) return 0;
    return Math.round((reactivated / total) * 100 * 10) / 10;
}

function calculateRiskPercentage(longTermInactive, activeUsers) {
    const total = longTermInactive + activeUsers;
    if (total === 0) return 0;
    return Math.round((longTermInactive / total) * 100 * 10) / 10;
}

// 차트 업데이트
function updateCharts() {
    console.log('차트 기능이 제거되었습니다.');
}

// 값 애니메이션
function animateValue(elementId, targetValue, suffix = '') {
    const element = document.getElementById(elementId);
    const startValue = 0;
    const duration = 1000;
    const startTime = performance.now();
    
    function updateValue(currentTime) {
        const elapsed = currentTime - startTime;
        const progress = Math.min(elapsed / duration, 1);
        
        const currentValue = startValue + (targetValue - startValue) * easeOutQuart(progress);
        
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

// 이징 함수
function easeOutQuart(t) {
    return 1 - Math.pow(1 - t, 4);
}

// 진행률 바 업데이트
function updateProgressBar(progress, message) {
    const progressBar = document.getElementById('progressBar');
    const progressText = document.getElementById('progressText');
    
    progressBar.style.width = progress + '%';
    progressBar.setAttribute('aria-valuenow', progress);
    progressText.textContent = message;
    
    // 진행률에 따른 색상 변경
    progressBar.className = 'progress-bar';
    if (progress < 30) {
        progressBar.classList.add('bg-info');
    } else if (progress < 70) {
        progressBar.classList.add('bg-warning');
    } else {
        progressBar.classList.add('bg-success');
    }
}

// 로그 추가 (개발자도구 콘솔로 출력)
function addLog(message, type = 'info') {
    const timestamp = getCurrentTime();
    const logMessage = `[${timestamp}] ${message}`;
    
    // 개발자도구 콘솔에 출력
    switch(type) {
        case 'error':
        case 'danger':
            console.error(logMessage);
            break;
        case 'warning':
            console.warn(logMessage);
            break;
        case 'success':
            console.log(`✅ ${logMessage}`);
            break;
        case 'info':
        default:
            console.log(`ℹ️ ${logMessage}`);
            break;
    }
}

// 현재 시간 가져오기
function getCurrentTime() {
    return new Date().toLocaleTimeString('ko-KR', { 
        hour12: false,
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });
}

// 알림 표시
function showAlert(message, type) {
    // 기존 알림 제거
    const existingAlert = document.querySelector('.alert');
    if (existingAlert) {
        existingAlert.remove();
    }
    
    const alert = document.createElement('div');
    alert.className = `alert alert-${type} alert-dismissible fade show`;
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    const main = document.querySelector('main');
    main.insertBefore(alert, main.firstChild);
    
    // 자동 제거
    setTimeout(() => {
        if (alert.parentNode) {
            alert.remove();
        }
    }, 5000);
}

// 파일 크기 포맷팅
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// 설정 변경 핸들러
function updateDateRange() {
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    
    if (startDate && endDate) {
        if (startDate > endDate) {
            showAlert('시작일이 종료일보다 늦을 수 없습니다.', 'warning');
            return;
        }
        
        // 날짜 차이 계산
        const start = new Date(startDate);
        const end = new Date(endDate);
        const diffDays = Math.ceil((end - start) / (1000 * 60 * 60 * 24));
        
        addLog(`분석 기간 변경: ${startDate} ~ ${endDate} (${diffDays}일)`, 'info');
        
        // 상태 카드 업데이트
        updateStatusCards();
    }
}


function updateSegmentOptions() {
    const gender = document.getElementById('genderSegment').checked;
    const age = document.getElementById('ageSegment').checked;
    const channel = document.getElementById('channelSegment').checked;
    
    const segments = [];
    if (gender) segments.push('성별');
    if (age) segments.push('연령대');
    if (channel) segments.push('채널');
    
    if (segments.length === 0) {
        addLog('세그먼트 옵션 변경: 모든 세그먼트 해제됨', 'info');
    } else {
        addLog(`세그먼트 옵션 변경: ${segments.join(', ')}`, 'info');
    }
    
    // 데이터가 있고 분석이 완료된 상태라면 메트릭을 즉시 업데이트
    if (window.csvData && window.csvData.length > 0) {
        // 메트릭도 다시 계산 (세그먼트 변경이 일부 계산에 영향을 줄 수 있음)
        updateMetricCards();
    }
}


// 키보드 단축키
document.addEventListener('keydown', function(e) {
    // Ctrl + Enter: 분석 실행
    if (e.ctrlKey && e.key === 'Enter') {
        e.preventDefault();
        if (!isAnalysisRunning) {
            runAnalysis();
        }
    }
    
    // Esc: 진행 중인 작업 취소 (향후 구현)
    if (e.key === 'Escape' && isAnalysisRunning) {
        // 취소 로직 구현 가능
    }
});

// 윈도우 리사이즈 시 차트 리사이즈
window.addEventListener('resize', function() {
    if (churnTrendChart) {
        churnTrendChart.resize();
    }
    if (segmentChart) {
        segmentChart.resize();
    }
});

// 실제 지표 계산 함수 (날짜 기반)
function calculateMetrics(data, config = {}) {
    const startDate = config.startDate || '2025-08-01';
    const endDate = config.endDate || '2025-10-31';

    const inactivityThreshold = 90;
    const endMonth = endDate.substring(0, 7);
    
    console.log(`[DEBUG] calculateMetrics 호출됨 - 기간: ${startDate} ~ ${endDate}`);
    console.log(`[DEBUG] 설정:`, config);
    console.log(`[DEBUG] 세그먼트 설정:`, config.segments);
    console.log(`[DEBUG] 데이터 행 수: ${data.length}`);
    console.log(`[DEBUG] 비활성 임계값: ${inactivityThreshold}일`);
    
    // 날짜 범위 내 데이터 필터링
    const filteredData = data.filter(row => {
        if (!row.created_at) return false;
        const rowDate = new Date(row.created_at).toISOString().split('T')[0];
        return rowDate >= startDate && rowDate <= endDate;
    });
    
    console.log(`[DEBUG] 필터링된 데이터: ${filteredData.length}행`);
    
    // 기간을 반으로 나누어 전반기/후반기 비교
    const start = new Date(startDate);
    const end = new Date(endDate);
    const midPoint = new Date(start.getTime() + (end.getTime() - start.getTime()) / 2);
    const midDateStr = midPoint.toISOString().split('T')[0];
    
    // 전반기와 후반기 사용자 분리
    const firstHalfUsers = new Set();
    const secondHalfUsers = new Set();
    
    filteredData.forEach(row => {
        const rowDate = new Date(row.created_at).toISOString().split('T')[0];
        if (rowDate <= midDateStr) {
            firstHalfUsers.add(row.user_hash);
        } else {
            secondHalfUsers.add(row.user_hash);
        }
    });
    
    const firstHalfCount = firstHalfUsers.size;
    const secondHalfCount = secondHalfUsers.size;
    
    console.log(`[DEBUG] 전반기 사용자: ${firstHalfCount}, 후반기 사용자: ${secondHalfCount}`);
    
    // 이탈률 계산 (전반기에는 있었지만 후반기에는 없는 사용자)
    let churnRate = 0;
    let churnedCount = 0;
    if (firstHalfCount > 0) {
        const churnedUsers = [...firstHalfUsers].filter(user => !secondHalfUsers.has(user));
        churnedCount = churnedUsers.length;
        churnRate = (churnedCount / firstHalfCount) * 100;
        console.log(`[DEBUG] 이탈자 수: ${churnedCount}, 이탈률: ${churnRate}%`);
    }
    
    // 재활성 사용자 계산 (후반기에만 있는 사용자)
    const reactivatedUsers = [...secondHalfUsers].filter(user => !firstHalfUsers.has(user)).length;
    
    // 장기 미접속 사용자 (설정된 임계값 기준)
    const longTermInactive = calculateLongTermInactive(data, endMonth, inactivityThreshold);
    
    const result = {
        churnRate: Math.round(churnRate * 10) / 10,
        activeUsers: secondHalfCount,
        previousActiveUsers: firstHalfCount,
        reactivatedUsers: reactivatedUsers,
        longTermInactive: longTermInactive,
        totalUsers: new Set(filteredData.map(row => row.user_hash)).size,
        totalEvents: filteredData.length
    };
    
    console.log(`[DEBUG] 최종 계산 결과:`, result);
    return result;
}

// 차트 데이터 계산 함수
function calculateChartData(data, config = {}) {
    console.log('[DEBUG] calculateChartData 호출됨, config:', config);
    
    // 날짜 기반 설정에서 월 추출
    const startDate = config.startDate || '2025-08-01';
    const endDate = config.endDate || '2025-10-31';
    const startMonth = startDate.substring(0, 7); // YYYY-MM
    const endMonth = endDate.substring(0, 7);
    
    console.log('[DEBUG] 차트 계산 기간:', startMonth, '~', endMonth);
    
    // 월 범위 생성
    const months = generateMonthRange(startMonth, endMonth);
    const trendData = [];
    
    console.log('[DEBUG] 생성된 월 범위:', months);
    
    // 데이터에서 실제 존재하는 월들 확인
    const availableMonths = [...new Set(data.map(row => row.year_month))].sort();
    console.log('[DEBUG] 데이터에 존재하는 월들:', availableMonths);
    
    for (let i = 1; i < months.length; i++) {
        const currentMonth = months[i];
        const previousMonth = months[i - 1];
        
        const activeUsersByMonth = {};
        data.forEach(row => {
            if (!activeUsersByMonth[row.year_month]) {
                activeUsersByMonth[row.year_month] = new Set();
            }
            activeUsersByMonth[row.year_month].add(row.user_hash);
        });
        
        const currentSet = activeUsersByMonth[currentMonth] || new Set();
        const previousSet = activeUsersByMonth[previousMonth] || new Set();
        
        console.log(`[DEBUG] ${currentMonth}: 현재 ${currentSet.size}명, 이전 ${previousSet.size}명`);
        
        let churnRate = 0;
        if (previousSet.size > 0) {
            const churnedUsers = [...previousSet].filter(user => !currentSet.has(user));
            churnRate = (churnedUsers.length / previousSet.size) * 100;
            console.log(`[DEBUG] ${currentMonth} 이탈률: ${churnRate}% (이탈자 ${churnedUsers.length}명)`);
        }
        
        trendData.push(Math.round(churnRate * 10) / 10);
    }
    
    // 세그먼트별 이탈률
    const segmentData = calculateSegmentChurnRates(data, config);
    
    const result = {
        trendLabels: months.slice(1), // 첫 번째 월 제외 (이탈률 계산 불가)
        trendData: trendData,
        segmentLabels: segmentData.labels,
        segmentData: segmentData.rates,
        segmentColors: segmentData.colors
    };
    
    console.log('[DEBUG] 차트 데이터 결과:', result);
    return result;
}

// 세그먼트별 이탈률 계산
function calculateSegmentChurnRates(data, config = {}) {
    console.log('[DEBUG] calculateSegmentChurnRates 호출됨, config:', config);
    
    const segments = {
        'M': '남성', 'F': '여성',
        '10s': '10대', '20s': '20대', '30s': '30대', '40s': '40대', '50s': '50대', '60s': '60대',
        'web': '웹', 'app': '앱'
    };
    
    // 날짜 기반 설정에서 월 추출
    const startDate = config.startDate || '2025-08-01';
    const endDate = config.endDate || '2025-10-31';
    const startMonth = startDate.substring(0, 7);
    const endMonth = endDate.substring(0, 7);
    
    // 분석할 월 범위 생성 (최소 2개월 필요)
    const months = generateMonthRange(startMonth, endMonth);
    if (months.length < 2) {
        console.log('[DEBUG] 세그먼트 분석을 위한 충분한 월 데이터가 없음');
        return { labels: [], rates: [], colors: [] };
    }
    
    console.log('[DEBUG] 세그먼트 분석 기간 전체:', months);
    
    const labels = [];
    const rates = [];
    const colors = [];
    
    // 성별 분석 (체크박스가 체크된 경우만) - 전체 기간 집계
    if (config.segments?.gender) {
        ['M', 'F'].forEach(gender => {
            const rate = calculateSegmentChurnRateFullRange(data, 'gender', gender, months);
            if (rate !== null) {
                labels.push(segments[gender]);
                rates.push(rate);
                colors.push(gender === 'M' ? '#007bff' : '#dc3545');
                console.log(`[DEBUG] ${segments[gender]} 이탈률 (전체 기간): ${rate}%`);
            }
        });
    }
    
    // 연령대 분석 (체크박스가 체크된 경우만) - 전체 기간 집계
    if (config.segments?.ageBand) {
        const availableAgeBands = ['10s', '20s', '30s', '40s', '50s', '60s'];
        const sortedAgeBands = sortAgeBands(availableAgeBands);
        
        const ageColors = ['#17a2b8', '#28a745', '#ffc107', '#fd7e14', '#dc3545', '#6f42c1'];
        
        sortedAgeBands.forEach(age => {
            const rate = calculateSegmentChurnRateFullRange(data, 'age_band', age, months);
            if (rate !== null) {
                labels.push(segments[age]);
                rates.push(rate);
                const colorIndex = sortedAgeBands.indexOf(age);
                colors.push(ageColors[colorIndex % ageColors.length]);
                console.log(`[DEBUG] ${segments[age]} 이탈률 (전체 기간): ${rate}%`);
            }
        });
    }
    
    // 채널 분석 (체크박스가 체크된 경우만) - 전체 기간 집계
    if (config.segments?.channel) {
        ['web', 'app'].forEach(channel => {
            const rate = calculateSegmentChurnRateFullRange(data, 'channel', channel, months);
            if (rate !== null) {
                labels.push(segments[channel]);
                rates.push(rate);
                colors.push(channel === 'web' ? '#6f42c1' : '#e83e8c');
                console.log(`[DEBUG] ${segments[channel]} 이탈률 (전체 기간): ${rate}%`);
            }
        });
    }
    
    console.log('[DEBUG] 세그먼트 분석 결과:', { labels, rates, colors });
    return { labels, rates, colors };
}

// 특정 세그먼트의 이탈률 계산 (전체 기간 집계)
function calculateSegmentChurnRateFullRange(data, segmentType, segmentValue, months) {
    const segmentData = data.filter(row => row[segmentType] === segmentValue);
    console.log(`[DEBUG] ${segmentType}=${segmentValue}: 필터된 데이터 ${segmentData.length}건`);
    
    const activeUsersByMonth = {};
    segmentData.forEach(row => {
        if (!activeUsersByMonth[row.year_month]) {
            activeUsersByMonth[row.year_month] = new Set();
        }
        activeUsersByMonth[row.year_month].add(row.user_hash);
    });
    
    let totalPreviousActive = 0;
    let totalChurned = 0;
    
    // 모든 월 전환에 대해 집계
    for (let i = 1; i < months.length; i++) {
        const previousMonth = months[i - 1];
        const currentMonth = months[i];
        
        const currentSet = activeUsersByMonth[currentMonth] || new Set();
        const previousSet = activeUsersByMonth[previousMonth] || new Set();
        
        if (previousSet.size > 0) {
            totalPreviousActive += previousSet.size;
            const churnedUsers = [...previousSet].filter(user => !currentSet.has(user));
            totalChurned += churnedUsers.length;
            
            console.log(`[DEBUG] ${segmentType}=${segmentValue}: ${previousMonth}->${currentMonth} 이전:${previousSet.size}명, 이탈:${churnedUsers.length}명`);
        }
    }
    
    if (totalPreviousActive === 0) {
        console.log(`[DEBUG] ${segmentType}=${segmentValue}: 전체 기간 이전 월 데이터 없음`);
        return null;
    }
    
    const churnRate = (totalChurned / totalPreviousActive) * 100;
    console.log(`[DEBUG] ${segmentType}=${segmentValue}: 전체 기간 이전활성:${totalPreviousActive}명, 총이탈:${totalChurned}명, 이탈률:${churnRate}%`);
    
    return Math.round(churnRate * 10) / 10;
}

// 특정 세그먼트의 이탈률 계산 (단일 월 전환)
function calculateSegmentChurnRate(data, segmentType, segmentValue, previousMonth, currentMonth) {
    const segmentData = data.filter(row => row[segmentType] === segmentValue);
    console.log(`[DEBUG] ${segmentType}=${segmentValue}: 필터된 데이터 ${segmentData.length}건`);
    
    const activeUsersByMonth = {};
    segmentData.forEach(row => {
        if (!activeUsersByMonth[row.year_month]) {
            activeUsersByMonth[row.year_month] = new Set();
        }
        activeUsersByMonth[row.year_month].add(row.user_hash);
    });
    
    const currentSet = activeUsersByMonth[currentMonth] || new Set();
    const previousSet = activeUsersByMonth[previousMonth] || new Set();
    
    console.log(`[DEBUG] ${segmentType}=${segmentValue}: ${previousMonth}월 ${previousSet.size}명, ${currentMonth}월 ${currentSet.size}명`);
    
    if (previousSet.size === 0) {
        console.log(`[DEBUG] ${segmentType}=${segmentValue}: 이전 월 데이터 없음`);
        return null; // 이전 월 데이터 없음
    }
    
    const churnedUsers = [...previousSet].filter(user => !currentSet.has(user));
    const churnRate = (churnedUsers.length / previousSet.size) * 100;
    
    console.log(`[DEBUG] ${segmentType}=${segmentValue}: 이탈자 ${churnedUsers.length}명, 이탈률 ${churnRate}%`);
    
    return Math.round(churnRate * 10) / 10;
}

// 재활성 사용자 계산 (30일 이상 쉬었다가 복귀)
function calculateReactivatedUsers(data, currentMonth) {
    const currentDate = new Date(currentMonth + '-01');
    const thirtyDaysAgo = new Date(currentDate.getTime() - 30 * 24 * 60 * 60 * 1000);
    
    // 현재 월 활성 사용자
    const currentActiveUsers = new Set();
    data.forEach(row => {
        if (row.year_month === currentMonth) {
            currentActiveUsers.add(row.user_hash);
        }
    });
    
    // 재활성 사용자 카운트
    let reactivatedCount = 0;
    currentActiveUsers.forEach(userId => {
        const userActivities = data.filter(row => row.user_hash === userId);
        userActivities.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
        
        // 현재 월 이전 마지막 활동 찾기
        let lastActivityBeforeCurrent = null;
        for (let activity of userActivities) {
            if (activity.year_month < currentMonth) {
                lastActivityBeforeCurrent = new Date(activity.created_at);
            }
        }
        
        if (lastActivityBeforeCurrent && (currentDate - lastActivityBeforeCurrent) >= 30 * 24 * 60 * 60 * 1000) {
            reactivatedCount++;
        }
    });
    
    return reactivatedCount;
}

// 장기 미접속 사용자 계산 (90일 이상)
function calculateLongTermInactive(data, currentMonth, inactivityDays = 90) {
    const thresholdDays = 90;

    const currentDate = new Date(currentMonth + '-01');

    if (Number.isNaN(currentDate.getTime())) {
        return 0;
    }

    const cutoffDate = new Date(currentDate.getTime() - thresholdDays * 24 * 60 * 60 * 1000);    

    // 모든 사용자의 마지막 활동일 계산
    const userLastActivity = {};
    data.forEach(row => {
        if (!row || !row.user_hash || !row.created_at) return;

        const activityDate = new Date(row.created_at);

        if (Number.isNaN(activityDate.getTime()) || activityDate > currentDate) return;

        const userId = row.user_hash;
        if (!userLastActivity[userId] || activityDate > userLastActivity[userId]) {
            userLastActivity[userId] = activityDate;
        }
    });
    
    // 임계값 이상 미접속 사용자 카운트
    let longTermInactiveCount = 0;
    Object.values(userLastActivity).forEach(lastActivity => {
        if (lastActivity < cutoffDate) {
            longTermInactiveCount++;
        }
    });
    
    return longTermInactiveCount;
}

// 현재 UI 설정 가져오기
function getCurrentConfig() {
    return {
        startDate: document.getElementById('startDate').value,
        endDate: document.getElementById('endDate').value,
        segments: {
            gender: document.getElementById('genderSegment').checked,
            ageBand: document.getElementById('ageSegment').checked,
            channel: document.getElementById('channelSegment').checked
        }
    };
}

// 월 범위 생성
function generateMonthRange(startMonth, endMonth) {
    const start = new Date(startMonth + '-01');
    const end = new Date(endMonth + '-01');
    const months = [];
    
    let current = new Date(start);
    while (current <= end) {
        const yearMonth = current.toISOString().substring(0, 7);
        months.push(yearMonth);
        current.setMonth(current.getMonth() + 1);
    }
    
    return months;
}

// 연령대 정렬 함수
function sortAgeBands(ageBands) {
    const ageOrder = ['10s', '20s', '30s', '40s', '50s', '60s'];
    return ageBands.sort((a, b) => {
        const aIndex = ageOrder.indexOf(a);
        const bIndex = ageOrder.indexOf(b);
        if (aIndex === -1 && bIndex === -1) return a.localeCompare(b);
        if (aIndex === -1) return 1;
        if (bIndex === -1) return -1;
        return aIndex - bIndex;
    });
}

// 연령대 표시 형식 변환 (10s → 10대)
function formatAgeBand(ageBand) {
    if (ageBand && ageBand.endsWith('s')) {
        return ageBand.replace('s', '대');
    }
    return ageBand;
}

// 이전 월 계산
function getPreviousMonth(month) {
    const date = new Date(month + '-01');
    date.setMonth(date.getMonth() - 1);
    return date.toISOString().substring(0, 7);
}

// 세그먼트 데이터 필터링
function filterSegmentsByConfig(chartData, segmentConfig) {
    const labels = [];
    const data = [];
    const colors = [];
    
    if (!chartData.segmentLabels) return { labels, data, colors };
    
    chartData.segmentLabels.forEach((label, index) => {
        let shouldInclude = false;
        
        // 성별 세그먼트
        if (segmentConfig.gender && (label === '남성' || label === '여성')) {
            shouldInclude = true;
        }
        
        // 연령대 세그먼트
        if (segmentConfig.ageBand && (label.includes('대'))) {
            shouldInclude = true;
        }
        
        // 채널 세그먼트
        if (segmentConfig.channel && (label === '웹' || label === '앱')) {
            shouldInclude = true;
        }
        
        if (shouldInclude) {
            labels.push(label);
            data.push(chartData.segmentData[index]);
            if (chartData.segmentColors) {
                colors.push(chartData.segmentColors[index]);
            }
        }
    });
    
    return { labels, data, colors };
}

// 세그먼트 분석 (완전 동적)
function calculateSegmentAnalysis(data, config) {
    const segments = {};
    
    // 날짜 범위에서 월 추출
    const startDate = config.startDate || '2025-08-01';
    const endDate = config.endDate || '2025-10-31';
    const startMonth = startDate.substring(0, 7);
    const endMonth = endDate.substring(0, 7);
    
    console.log(`[DEBUG] calculateSegmentAnalysis - 기간: ${startMonth} ~ ${endMonth}`);
    
    // 날짜 범위 내 데이터 필터링
    const filteredData = data.filter(row => {
        if (!row.created_at) return false;
        const rowDate = new Date(row.created_at).toISOString().split('T')[0];
        return rowDate >= startDate && rowDate <= endDate;
    });
    
    console.log(`[DEBUG] 세그먼트 분석용 필터링된 데이터: ${filteredData.length}행`);
    
    // 성별 분석
    if (config.segments.gender) {
        segments.gender = analyzeSegmentByTypeFullRange(filteredData, 'gender', ['M', 'F'], startMonth, endMonth);
    }
    
    // 연령대 분석
    if (config.segments.ageBand) {
        const ageBands = [...new Set(filteredData.map(row => row.age_band).filter(age => age && age !== 'Unknown'))];
        const sortedAgeBands = sortAgeBands(ageBands);
        segments.age_band = analyzeSegmentByTypeFullRange(filteredData, 'age_band', sortedAgeBands, startMonth, endMonth);
    }
    
    // 채널 분석
    if (config.segments.channel) {
        segments.channel = analyzeSegmentByTypeFullRange(filteredData, 'channel', ['web', 'app'], startMonth, endMonth);
    }
    
    return segments;
}

// 특정 타입의 세그먼트 분석 (전체 기간 집계)
function analyzeSegmentByTypeFullRange(data, segmentType, segmentValues, startMonth, endMonth) {
    const results = [];
    
    // 분석할 월 범위 생성
    const months = generateMonthRange(startMonth, endMonth);
    if (months.length < 2) {
        console.log(`[DEBUG] ${segmentType} 분석: 충분한 월 데이터가 없음`);
        return [];
    }
    
    console.log(`[DEBUG] ${segmentType} 분석 기간:`, months);
    
    segmentValues.forEach(segmentValue => {
        // 해당 세그먼트의 데이터만 필터링
        const segmentData = data.filter(row => row[segmentType] === segmentValue);
        
        if (segmentData.length === 0) return;
        
        // 월별 활성 사용자 계산
        const activeUsersByMonth = {};
        segmentData.forEach(row => {
            if (!activeUsersByMonth[row.year_month]) {
                activeUsersByMonth[row.year_month] = new Set();
            }
            activeUsersByMonth[row.year_month].add(row.user_hash);
        });
        
        let totalPreviousActive = 0;
        let totalChurned = 0;
        let totalCurrentActive = 0;
        
        // 모든 월 전환에 대해 집계
        for (let i = 1; i < months.length; i++) {
            const previousMonth = months[i - 1];
            const currentMonth = months[i];
            
            const currentSet = activeUsersByMonth[currentMonth] || new Set();
            const previousSet = activeUsersByMonth[previousMonth] || new Set();
            
            if (previousSet.size > 0) {
                totalPreviousActive += previousSet.size;
                const churnedUsers = [...previousSet].filter(user => !currentSet.has(user));
                totalChurned += churnedUsers.length;
                
                console.log(`[DEBUG] ${segmentType}=${segmentValue}: ${previousMonth}->${currentMonth} 이전:${previousSet.size}명, 이탈:${churnedUsers.length}명`);
            }
            
            // 마지막 월의 현재 활성 사용자 수
            if (i === months.length - 1) {
                totalCurrentActive = currentSet.size;
            }
        }
        
        if (totalPreviousActive === 0) {
            console.log(`[DEBUG] ${segmentType}=${segmentValue}: 전체 기간 이전 월 데이터 없음`);
            return;
        }
        
        const churnRate = (totalChurned / totalPreviousActive) * 100;
        const isUncertain = totalPreviousActive < 30;
        
        console.log(`[DEBUG] ${segmentType}=${segmentValue}: 전체 기간 이전활성:${totalPreviousActive}명, 총이탈:${totalChurned}명, 이탈률:${churnRate}%`);
        
        results.push({
            segment_value: segmentValue,
            current_active: totalCurrentActive,
            previous_active: totalPreviousActive,
            churned_users: totalChurned,
            churn_rate: Math.round(churnRate * 10) / 10,
            is_uncertain: isUncertain
        });
    });
    
    // 연령대의 경우 연령 순서로 정렬, 다른 세그먼트는 이탈률 높은 순으로 정렬
    if (segmentType === 'age_band') {
        return results.sort((a, b) => {
            const ageOrder = ['10s', '20s', '30s', '40s', '50s', '60s'];
            const aIndex = ageOrder.indexOf(a.segment_value);
            const bIndex = ageOrder.indexOf(b.segment_value);
            if (aIndex === -1 && bIndex === -1) return a.segment_value.localeCompare(b.segment_value);
            if (aIndex === -1) return 1;
            if (bIndex === -1) return -1;
            return aIndex - bIndex;
        });
    } else {
        return results.sort((a, b) => b.churn_rate - a.churn_rate);
    }
}

// 특정 타입의 세그먼트 분석 (단일 월 비교 - 기존 함수 유지)
function analyzeSegmentByType(data, segmentType, segmentValues, currentMonth, previousMonth) {
    const results = [];
    
    segmentValues.forEach(segmentValue => {
        // 해당 세그먼트의 데이터만 필터링
        const segmentData = data.filter(row => row[segmentType] === segmentValue);
        
        if (segmentData.length === 0) return;
        
        // 월별 활성 사용자 계산
        const activeUsersByMonth = {};
        segmentData.forEach(row => {
                if (!activeUsersByMonth[row.year_month]) {
                    activeUsersByMonth[row.year_month] = new Set();
                }
                activeUsersByMonth[row.year_month].add(row.user_hash);
        });
        
        const currentActive = activeUsersByMonth[currentMonth] ? activeUsersByMonth[currentMonth].size : 0;
        const previousActive = activeUsersByMonth[previousMonth] ? activeUsersByMonth[previousMonth].size : 0;
        
        if (previousActive === 0) return;
        
        // 이탈 사용자 계산
        const currentSet = activeUsersByMonth[currentMonth] || new Set();
        const previousSet = activeUsersByMonth[previousMonth] || new Set();
        const churnedUsers = [...previousSet].filter(user => !currentSet.has(user));
        const churnRate = (churnedUsers.length / previousActive) * 100;
        
        // Uncertain 판정 (모수가 적으면)
        const isUncertain = previousActive < 30;
        
        results.push({
            segment_value: segmentValue,
            current_active: currentActive,
            previous_active: previousActive,
            churned_users: churnedUsers.length,
            churn_rate: Math.round(churnRate * 10) / 10,
            is_uncertain: isUncertain
        });
    });
    
    return results.sort((a, b) => b.churn_rate - a.churn_rate);
}

// 데이터 품질 계산 (동적)
function calculateDataQuality(data) {
    if (!data || data.length === 0) {
        return {
            total_events: 0,
            invalid_events: 0,
            data_completeness: 0,
            unknown_ratio: 0
        };
    }
    
    const totalEvents = data.length;
    let unknownCount = 0;
    let invalidCount = 0;
    
    data.forEach(row => {
        // Unknown 값 카운트
        if (row.gender === 'Unknown' || row.age_band === 'Unknown' || row.channel === 'Unknown') {
            unknownCount++;
        }
        
        // 필수 필드 누락 체크
        if (!row.user_hash || !row.created_at || !row.action) {
            invalidCount++;
        }
    });
    
    const uniqueUsers = new Set(data.map(row => row.user_hash)).size;
    
    return {
        total_events: totalEvents,
        invalid_events: invalidCount,
        data_completeness: Math.round(((totalEvents - invalidCount) / totalEvents) * 100 * 10) / 10,
        unknown_ratio: Math.round((unknownCount / totalEvents) * 100 * 10) / 10,
        unique_users: uniqueUsers
    };
}

// 동적 인사이트 생성
// [DEPRECATED] 기존 하드코딩된 인사이트 생성 - LLM으로 대체됨
function generateDynamicInsights(metrics, segmentAnalysis, chartData) {
    const inactivityThreshold = getInactiveThresholdValue();
    const insights = [];
    
    // 1. 전체 이탈률 트렌드 인사이트
    if (chartData.trendData && chartData.trendData.length >= 2) {
        const currentRate = chartData.trendData[chartData.trendData.length - 1];
        const previousRate = chartData.trendData[chartData.trendData.length - 2];
        const change = currentRate - previousRate;
        
        if (Math.abs(change) > 1) {
            const direction = change > 0 ? '상승' : '하락';
            insights.push(`이탈률이 전월 대비 ${Math.abs(change).toFixed(1)}%p ${direction}했습니다.`);
        }
    }
    
    // 2. 세그먼트별 인사이트 (성별)
    if (segmentAnalysis.gender && segmentAnalysis.gender.length >= 2) {
        const genderData = segmentAnalysis.gender;
        const highest = genderData[0];
        const lowest = genderData[genderData.length - 1];
        const diff = highest.churn_rate - lowest.churn_rate;
        
        if (diff > 3) {
            const highName = highest.segment_value === 'M' ? '남성' : '여성';
            const lowName = lowest.segment_value === 'M' ? '남성' : '여성';
            const uncertainNote = highest.is_uncertain ? ' (모수 부족으로 Uncertain 표기)' : '';
            
            insights.push(`${highName} 사용자의 이탈률이 ${lowName} 대비 ${diff.toFixed(1)}%p 높습니다${uncertainNote}.`);
        }
    }
    
    // 3. 연령대별 인사이트
    if (segmentAnalysis.age_band && segmentAnalysis.age_band.length > 0) {
        const ageData = segmentAnalysis.age_band;
        const highestAge = ageData[0];
        
        if (highestAge.churn_rate > 20) {
            const uncertainNote = highestAge.is_uncertain ? ' (모수 부족으로 Uncertain 표기)' : '';
            insights.push(`${highestAge.segment_value} 세그먼트에서 높은 이탈률(${highestAge.churn_rate}%)을 보입니다${uncertainNote}.`);
        }
    }
    
    // 4. 장기 미접속 인사이트
    if ((metrics.longTermInactive || 0) > 0 && (metrics.activeUsers || 0) > 0) {
        const inactiveRatio = (metrics.longTermInactive / ((metrics.activeUsers || 0) + metrics.longTermInactive)) * 100;
        if (inactiveRatio > 10) {
            insights.push(`⏳ ${inactivityThreshold}일 이상 미접속 사용자가 전체의 ${inactiveRatio.toFixed(1)}%입니다. 복귀 전략을 검토하세요.`);
        }
    }
    
    // 최대 3개까지만 반환
    return insights.slice(0, 3);
}

// 동적 액션 생성
// [DEPRECATED] 기존 하드코딩된 액션 생성 - LLM으로 대체됨
function generateDynamicActions(insights, segmentAnalysis, metrics) {
    const actions = [];
    
    // 1. 세그먼트별 액션
    if (segmentAnalysis.gender) {
        const femaleData = segmentAnalysis.gender.find(s => s.segment_value === 'F');
        const maleData = segmentAnalysis.gender.find(s => s.segment_value === 'M');
        
        if (femaleData && maleData && femaleData.churn_rate > maleData.churn_rate + 5) {
            actions.push('여성 사용자 대상 맞춤형 콘텐츠 및 커뮤니티 활동 강화');
        }
    }
    
    // 2. 연령대별 액션
    if (segmentAnalysis.age_band) {
        const highChurnAge = segmentAnalysis.age_band.find(s => s.churn_rate > 25);
        if (highChurnAge) {
            if (highChurnAge.segment_value.includes('50') || highChurnAge.segment_value.includes('60')) {
                actions.push('50대 이상 사용자를 위한 사용성 개선 및 신규 가이드 제공');
            } else {
                actions.push(`${highChurnAge.segment_value} 사용자를 위한 맞춤형 서비스 개선`);
            }
        }
    }
    
    // 3. 채널별 액션
    if (segmentAnalysis.channel) {
        const appData = segmentAnalysis.channel.find(s => s.segment_value === 'app');
        const webData = segmentAnalysis.channel.find(s => s.segment_value === 'web');
        
        if (appData && webData && appData.churn_rate > webData.churn_rate + 3) {
            actions.push('모바일 앱 사용자 경험 개선 및 푸시 알림 최적화');
        }
    }
    
    // 4. 일반적인 액션
    if (metrics.longTermInactive > 0) {
        actions.push('장기 미접속자 대상 복귀 유도 캠페인 및 개인화된 콘텐츠 추천');
    }
    
    // 5. 재활성 사용자 관련 액션
    if (metrics.reactivatedUsers > 0) {
        actions.push('재활성 사용자 패턴 분석을 통한 이탈 방지 전략 수립');
    }
    
    // 최대 3개까지만 반환
    return actions.slice(0, 3);
}

// 리포트를 동적 데이터로 업데이트
async function updateReportWithDynamicData() {
    if (!window.csvData || window.csvData.length === 0) {
        updateReportSection(['데이터가 없습니다.'], ['데이터를 업로드해주세요.'], { total_events: 0, invalid_events: 0, data_completeness: 0, unknown_ratio: 0 });
        return;
    }
    
    const config = getCurrentConfig();
    
    // 실제 계산된 메트릭과 세그먼트 데이터 사용 (백엔드 또는 프론트엔드)
    let finalMetrics;
    let segmentAnalysis;
    try {
        // 먼저 백엔드 API에서 메트릭과 세그먼트 데이터 가져오기 시도
        const backendResponse = await callBackendAPI(config);
        const isBackendDataEmpty = backendResponse.error || 
            (backendResponse.churn_rate === 0 && backendResponse.active_users === 0 && 
             backendResponse.reactivated_users === 0 && backendResponse.long_term_inactive === 0);
        
        if (isBackendDataEmpty) {
            // 백엔드 실패 시 프론트엔드 계산 사용
            finalMetrics = calculateMetrics(window.csvData, config);
            segmentAnalysis = calculateSegmentAnalysis(window.csvData, config);
            addLog('리포트: 로컬 계산된 메트릭 사용', 'info');
        } else {
            // 백엔드 성공 시 백엔드 메트릭과 세그먼트 데이터 사용
            finalMetrics = {
                churnRate: backendResponse.churn_rate,
                activeUsers: backendResponse.active_users,
                reactivatedUsers: backendResponse.reactivated_users,
                longTermInactive: backendResponse.long_term_inactive,
                previousActiveUsers: backendResponse.previous_active_users || 0
            };
            
            // 백엔드에서 세그먼트 분석 결과가 있으면 사용, 없으면 프론트엔드 계산
            if (backendResponse.segments) {
                segmentAnalysis = backendResponse.segments;
                addLog('리포트: 백엔드 세그먼트 분석 결과 사용', 'success');
            } else {
                segmentAnalysis = calculateSegmentAnalysis(window.csvData, config);
                addLog('리포트: 백엔드 메트릭 + 로컬 세그먼트 분석 사용', 'info');
            }
            addLog('리포트: 백엔드 계산된 메트릭 사용', 'success');
        }
    } catch (error) {
        // API 호출 실패 시 프론트엔드 계산 사용
        finalMetrics = calculateMetrics(window.csvData, config);
        segmentAnalysis = calculateSegmentAnalysis(window.csvData, config);
        addLog('리포트: API 실패로 로컬 메트릭 사용', 'warning');
    }
    
    console.log('[DEBUG] 리포트용 최종 메트릭:', finalMetrics);
    console.log('[DEBUG] 리포트용 세그먼트 분석:', segmentAnalysis);
    
    const chartData = calculateChartData(window.csvData, config);
    const dataQuality = calculateDataQuality(window.csvData);
    
    // 백엔드 API 호출하여 LLM 기반 인사이트 생성 (실제 메트릭 포함)
    try {
        addLog('AI 인사이트 생성 중...', 'info');
        
        const response = await fetch('http://localhost:8000/analysis/run', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                start_month: config.startDate.substring(0, 7), // YYYY-MM 형식
                end_month: config.endDate.substring(0, 7),
                segments: {
                    gender: config.segments.gender,
                    age_band: config.segments.ageBand,
                    channel: config.segments.channel
                },
                // 실제 계산된 메트릭 전달
                calculated_metrics: {
                    churn_rate: finalMetrics.churnRate,
                    active_users: finalMetrics.activeUsers,
                    reactivated_users: finalMetrics.reactivatedUsers,
                    long_term_inactive: finalMetrics.longTermInactive,
                    previous_active_users: finalMetrics.previousActiveUsers
                }
            })
        });
        
        if (response.ok) {
            const result = await response.json();
            
            addLog('✅ AI 분석 완료!', 'success');
            
            updateReportSection(
                result.insights || [],
                result.actions || [],
                dataQuality,
                result.llm_metadata,
                segmentAnalysis
            );
        } else {
            throw new Error(`API 호출 실패: ${response.status}`);
        }
        
    } catch (error) {
        addLog(`❌ AI 인사이트 생성 실패: ${error.message}`, 'error');
        
        // AI 실패 시 기본 분석 기반 인사이트 생성 (세그먼트 분석 결과 활용)
        const basicInsights = generateBasicInsights(finalMetrics, segmentAnalysis, chartData, config);
        const basicActions = generateBasicActions(finalMetrics, segmentAnalysis);
        
        const fallbackMetadata = {
            generation_method: 'basic_analysis',
            fallback_used: true,
            setup_required: false,
            error: error.message,
            timestamp: new Date().toISOString()
        };
        
        updateReportSection(basicInsights, basicActions, dataQuality, fallbackMetadata, segmentAnalysis);
    }
    
    addLog('리포트 업데이트 완료', 'success');
}

// 기본 인사이트 생성 (AI 실패 시 사용)
function generateBasicInsights(metrics, segmentAnalysis, chartData, config = {}) {
    const inactivityThreshold = 90;
    const insights = [];
    
    // 1. 전체 이탈률 인사이트
    const churnRate = metrics.churnRate || 0;
    if (churnRate > 25) {
        insights.push(`⚠️ 전체 이탈률이 ${churnRate.toFixed(1)}%로 매우 높은 수준입니다.`);
    } else if (churnRate > 15) {
        insights.push(`📊 전체 이탈률이 ${churnRate.toFixed(1)}%로 주의가 필요한 수준입니다.`);
    } else {
        insights.push(`✅ 전체 이탈률이 ${churnRate.toFixed(1)}%로 양호한 수준입니다.`);
    }
    
    // 2. 활성 사용자 인사이트
    const activeUsers = metrics.activeUsers || 0;
    const previousUsers = metrics.previousActiveUsers || 0;
    if (previousUsers > 0) {
        const growth = ((activeUsers - previousUsers) / previousUsers * 100);
        if (growth > 5) {
            insights.push(`📈 활성 사용자가 ${activeUsers.toLocaleString()}명으로 ${growth.toFixed(1)}% 증가했습니다.`);
        } else if (growth < -5) {
            insights.push(`📉 활성 사용자가 ${activeUsers.toLocaleString()}명으로 ${Math.abs(growth).toFixed(1)}% 감소했습니다.`);
        } else {
            insights.push(`📊 활성 사용자가 ${activeUsers.toLocaleString()}명으로 안정적인 수준입니다.`);
        }
    } else {
        insights.push(`👥 현재 활성 사용자는 ${activeUsers.toLocaleString()}명입니다.`);
    }
    
    // 3. 세그먼트 인사이트 (선택된 세그먼트가 있는 경우)
    if (segmentAnalysis && Object.keys(segmentAnalysis).length > 0) {
        console.log('[DEBUG] 세그먼트 분석 결과:', segmentAnalysis);
        
        // 성별 분석
        if (segmentAnalysis.gender && segmentAnalysis.gender.length >= 2) {
            const genderData = segmentAnalysis.gender;
            const highest = genderData.reduce((prev, current) => 
                (prev.churn_rate > current.churn_rate) ? prev : current
            );
            const lowest = genderData.reduce((prev, current) => 
                (prev.churn_rate < current.churn_rate) ? prev : current
            );
            
            if (Math.abs(highest.churn_rate - lowest.churn_rate) > 3) {
                const highName = highest.segment_value === 'M' ? '남성' : '여성';
                const lowName = lowest.segment_value === 'M' ? '남성' : '여성';
                const uncertainNote = highest.is_uncertain ? ' (모수 부족)' : '';
                insights.push(`👥 ${highName} 사용자의 이탈률(${highest.churn_rate.toFixed(1)}%)이 ${lowName}(${lowest.churn_rate.toFixed(1)}%) 대비 높습니다${uncertainNote}.`);
            }
        }
        
        // 연령대 분석
        if (segmentAnalysis.age_band && segmentAnalysis.age_band.length > 0) {
            const ageData = segmentAnalysis.age_band;
            const highestAge = ageData.reduce((prev, current) => 
                (prev.churn_rate > current.churn_rate) ? prev : current
            );
            
            if (highestAge.churn_rate > 20) {
                const uncertainNote = highestAge.is_uncertain ? ' (모수 부족으로 Uncertain 표기)' : '';
                insights.push(`🎯 ${highestAge.segment_value} 연령대에서 높은 이탈률(${highestAge.churn_rate.toFixed(1)}%)을 보입니다${uncertainNote}.`);
            }
        }
        
        // 채널 분석
        if (segmentAnalysis.channel && segmentAnalysis.channel.length >= 2) {
            const channelData = segmentAnalysis.channel;
            const highest = channelData.reduce((prev, current) => 
                (prev.churn_rate > current.churn_rate) ? prev : current
            );
            const lowest = channelData.reduce((prev, current) => 
                (prev.churn_rate < current.churn_rate) ? prev : current
            );
            
            if (Math.abs(highest.churn_rate - lowest.churn_rate) > 5) {
                const highName = highest.segment_value === 'app' ? '모바일 앱' : 
                               highest.segment_value === 'web' ? '웹' : highest.segment_value;
                const lowName = lowest.segment_value === 'app' ? '모바일 앱' : 
                               lowest.segment_value === 'web' ? '웹' : lowest.segment_value;
                insights.push(`📱 ${highName} 채널의 이탈률(${highest.churn_rate.toFixed(1)}%)이 ${lowName}(${lowest.churn_rate.toFixed(1)}%) 대비 높습니다.`);
            }
        }
    }
    
    return insights.slice(0, 3); // 최대 3개
}

// 기본 액션 생성 (AI 실패 시 사용)
function generateBasicActions(metrics, segmentAnalysis) {
    const actions = [];
    
    // 1. 이탈률 기반 액션
    const churnRate = metrics.churnRate || 0;
    if (churnRate > 20) {
        actions.push("🚨 긴급 이탈 방지 프로그램 도입 및 사용자 피드백 수집");
    } else if (churnRate > 15) {
        actions.push("📋 이탈 위험 사용자 식별 및 맞춤형 리텐션 캠페인 실행");
    } else {
        actions.push("✨ 현재 서비스 품질 유지 및 사용자 만족도 지속 모니터링");
    }
    
    // 2. 세그먼트 기반 액션
    if (segmentAnalysis && Object.keys(segmentAnalysis).length > 0) {
        // 성별 세그먼트 액션
        if (segmentAnalysis.gender && segmentAnalysis.gender.length >= 2) {
            const genderData = segmentAnalysis.gender;
            const highest = genderData.reduce((prev, current) => 
                (prev.churn_rate > current.churn_rate) ? prev : current
            );
            
            if (highest.churn_rate > 15) {
                const targetGender = highest.segment_value === 'M' ? '남성' : '여성';
                actions.push(`👥 ${targetGender} 사용자 대상 맞춤형 콘텐츠 및 서비스 개선`);
            }
        }
        
        // 연령대 세그먼트 액션
        if (segmentAnalysis.age_band && segmentAnalysis.age_band.length > 0) {
            const ageData = segmentAnalysis.age_band;
            const highestAge = ageData.reduce((prev, current) => 
                (prev.churn_rate > current.churn_rate) ? prev : current
            );
            
            if (highestAge.churn_rate > 18) {
                actions.push(`🎯 ${highestAge.segment_value} 연령대를 위한 전용 서비스 및 UI/UX 개선`);
            }
        }
        
        // 채널 세그먼트 액션
        if (segmentAnalysis.channel && segmentAnalysis.channel.length >= 2) {
            const channelData = segmentAnalysis.channel;
            const highest = channelData.reduce((prev, current) => 
                (prev.churn_rate > current.churn_rate) ? prev : current
            );
            
            if (highest.churn_rate > 15) {
                const targetChannel = highest.segment_value === 'web' ? '웹' : '앱';
                actions.push(`📱 ${targetChannel} 플랫폼 사용자 경험 개선 및 기능 최적화`);
            }
        }
    }
    
    // 3. 일반적인 액션
    const reactivationRate = metrics.reactivatedUsers / (metrics.longTermInactive + metrics.reactivatedUsers) * 100;
    if (reactivationRate < 10) {
        actions.push("🔄 장기 미접속자 대상 복귀 유도 캠페인 및 개인화된 알림 시스템 구축");
    }
    
    return actions.slice(0, 3); // 최대 3개
}

// 리포트 섹션 업데이트 (기존 함수 개선)
function updateReportSection(insights, actions, dataQuality, llmMetadata = null, segmentAnalysis = null) {
    // 인사이트 업데이트
    const insightsContainer = document.querySelector('#report .mb-4:first-child ul');
    if (insightsContainer && insights && insights.length > 0) {
        insightsContainer.innerHTML = insights.map((insight, index) => {
            const colors = ['text-danger', 'text-warning', 'text-info'];
            const color = colors[index] || 'text-info';
            return `<li class="mb-2">
                <i class="fas fa-circle ${color} me-2" style="font-size: 0.5rem;"></i>
                ${insight}
            </li>`;
        }).join('');
    } else if (insightsContainer) {
        insightsContainer.innerHTML = '<li class="mb-2"><i class="fas fa-circle text-muted me-2" style="font-size: 0.5rem;"></i>분석할 데이터가 부족합니다.</li>';
    }

    // 액션 업데이트
    const actionsContainer = document.querySelector('#report .mb-4:nth-child(2) ul');
    if (actionsContainer && actions && actions.length > 0) {
        actionsContainer.innerHTML = actions.map(action => 
            `<li class="mb-2">
                <i class="fas fa-arrow-right text-success me-2"></i>
                ${action}
            </li>`
        ).join('');
    } else if (actionsContainer) {
        actionsContainer.innerHTML = '<li class="mb-2"><i class="fas fa-arrow-right text-muted me-2"></i>권장 액션을 생성할 수 없습니다.</li>';
    }

    // 세그먼트 분석 결과 업데이트
    if (segmentAnalysis && Object.keys(segmentAnalysis).length > 0) {
        const segmentContainer = document.querySelector('#report .mb-4:nth-child(3) ul');
        if (segmentContainer) {
            let segmentHtml = '';
            
            // 성별 분석 결과
            if (segmentAnalysis.gender && segmentAnalysis.gender.length > 0) {
                segmentHtml += '<li class="mb-2"><strong>성별 이탈률:</strong></li>';
                segmentAnalysis.gender.forEach(segment => {
                    const genderName = segment.segment_value === 'M' ? '남성' : '여성';
                    const uncertainNote = segment.is_uncertain ? ' (Uncertain)' : '';
                    segmentHtml += `<li class="mb-1 ms-3">• ${genderName}: ${segment.churn_rate.toFixed(1)}% (활성: ${segment.current_active}명)${uncertainNote}</li>`;
                });
            }
            
            // 연령대 분석 결과
            if (segmentAnalysis.age_band && segmentAnalysis.age_band.length > 0) {
                segmentHtml += '<li class="mb-2"><strong>연령대별 이탈률:</strong></li>';
                segmentAnalysis.age_band.forEach(segment => {
                    const uncertainNote = segment.is_uncertain ? ' (Uncertain)' : '';
                    const formattedAge = formatAgeBand(segment.segment_value);
                    segmentHtml += `<li class="mb-1 ms-3">• ${formattedAge}: ${segment.churn_rate.toFixed(1)}% (활성: ${segment.current_active}명)${uncertainNote}</li>`;
                });
            }
            
            // 채널 분석 결과
            if (segmentAnalysis.channel && segmentAnalysis.channel.length > 0) {
                segmentHtml += '<li class="mb-2"><strong>채널별 이탈률:</strong></li>';
                segmentAnalysis.channel.forEach(segment => {
                    const channelName = segment.segment_value === 'app' ? '모바일 앱' : 
                                      segment.segment_value === 'web' ? '웹' : segment.segment_value;
                    const uncertainNote = segment.is_uncertain ? ' (Uncertain)' : '';
                    segmentHtml += `<li class="mb-1 ms-3">• ${channelName}: ${segment.churn_rate.toFixed(1)}% (활성: ${segment.current_active}명)${uncertainNote}</li>`;
                });
            }
            
            if (segmentHtml) {
                segmentContainer.innerHTML = segmentHtml;
            }
        }
    }

    // 데이터 품질 업데이트
    if (dataQuality) {
        const qualityContainer = document.querySelector('#report .mb-4:nth-child(4) ul');
        if (qualityContainer) {
            qualityContainer.innerHTML = `
                <li class="mb-1">• 총 ${dataQuality.total_events.toLocaleString()}행 검증 완료 (${dataQuality.invalid_events}행 제외)</li>
                <li class="mb-1">• 고유 사용자: ${dataQuality.unique_users ? dataQuality.unique_users.toLocaleString() : 0}명</li>
                <li class="mb-1">• 데이터 완전성: ${dataQuality.data_completeness}%</li>
                <li class="mb-1">• Unknown 값 비율: ${dataQuality.unknown_ratio}%</li>
            `;
        }
        
    }
    
}

