// Global state
let charts = {};
let sse = null;
let refreshInterval = 5000; // default 5s
let metricsBuffer = [];
let liveStatsJsonPath = 'live_stats.json'; // Path to generated JSON file
let fetchIntervalId = null; // Track the fetch interval
let selectedScenario = 'all'; // Current scenario filter
let selectedRequest = 'all'; // Current request filter
let selectedHostname = 'all'; // Current hostname filter
let rawLiveStatsData = null; // Store raw JSON data for filtering

// Color palette for charts
const chartColors = [
    'rgba(75, 192, 192, 1)',   // teal
    'rgba(255, 99, 132, 1)',   // red
    'rgba(54, 162, 235, 1)',   // blue
    'rgba(255, 206, 86, 1)',   // yellow
    'rgba(153, 102, 255, 1)',  // purple
    'rgba(255, 159, 64, 1)',   // orange
    'rgba(46, 204, 113, 1)',   // green
    'rgba(231, 76, 60, 1)',    // dark red
    'rgba(52, 152, 219, 1)',   // sky blue
    'rgba(241, 196, 15, 1)',   // gold
    'rgba(155, 89, 182, 1)',   // violet
    'rgba(26, 188, 156, 1)',   // turquoise
];

// Get random color from palette
function getRandomColor() {
    const randomIndex = Math.floor(Math.random() * chartColors.length);
    return chartColors[randomIndex];
}

// Create a chart with full dynamic configuration
function createChart(canvasElement, type, xValues, yValues, xLabel, yLabel) {
    if (!canvasElement) {
        console.warn('Canvas element not found');
        return null;
    }

    const randomColor = getRandomColor();
    const backgroundColor = randomColor.replace('1)', '0.2)');

    return new Chart(canvasElement, {
        type: type,
        data: {
            labels: xValues || [],
            datasets: [{
                label: yLabel || 'Data',
                data: yValues || [],
                borderColor: randomColor,
                backgroundColor: backgroundColor,
                borderWidth: 2,
                tension: 0.1,
                fill: false
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    mode: 'index',
                    intersect: false
                }
            },
            interaction: {
                mode: 'nearest',
                axis: 'x',
                intersect: false
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: xLabel || 'X Axis'
                    }
                },
                y: {
                    display: true,
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: yLabel || 'Y Axis'
                    }
                }
            }
        }
    });
}

// Initialize all charts with empty data
function initCharts() {
    charts.reqRespChart = createChart(document.getElementById('reqRespChart'), 'line', [], [], 'Time', 'Requests/s');
    charts.respStatusChart = createChart(document.getElementById('respStatusChart'), 'bar', [], [], 'Status', 'Count');
    charts.respTimeChart = createChart(document.getElementById('respTimeChart'), 'line', [], [], 'Time', 'Response Time (ms)');
    charts.errorsChart = createChart(document.getElementById('errorsChart'), 'line', [], [], 'Time', 'Errors/s');
    charts.arrivalChart = createChart(document.getElementById('arrivalChart'), 'line', [], [], 'Time', 'Users/s');
    charts.terminationChart = createChart(document.getElementById('terminationChart'), 'line', [], [], 'Time', 'Users/s');
    charts.concurrentChart = createChart(document.getElementById('concurrentChart'), 'line', [], [], 'Time', 'Active Users');
    charts.connOpenCloseChart = createChart(document.getElementById('connOpenCloseChart'), 'line', [], [], 'Time', 'Connections/s');
    charts.tcpStateChart = createChart(document.getElementById('tcpStateChart'), 'bar', [], [], 'State', 'Connections');
    charts.tcpDurationChart = createChart(document.getElementById('tcpDurationChart'), 'line', [], [], 'Time', 'Duration (ms)');
    charts.tlsChart = createChart(document.getElementById('tlsChart'), 'line', [], [], 'Time', 'TLS Handshake (ms)');
    charts.bandwidthChart = createChart(document.getElementById('bandwidthChart'), 'line', [], [], 'Time', 'Bandwidth (MB/s)');
    charts.dnsResChart = createChart(document.getElementById('dnsResChart'), 'line', [], [], 'Time', 'Resolutions/s');
    charts.dnsDurationChart = createChart(document.getElementById('dnsDurationChart'), 'line', [], [], 'Time', 'DNS Lookup (ms)');
}

// Connect to SSE server
function connectSSE() {
    if (sse) sse.close();

    sse = new EventSource('/events');
    sse.onmessage = function (event) {
        try {
            const raw = JSON.parse(event.data);
            const metric = convertRawMetricToDashboardMetric(raw);
            metricsBuffer.push(metric);
            updateJsonViewer(metric);
        } catch (e) {
            console.error("Invalid SSE payload", e);
        }
    };
}

// Fetch JSON file generated by CsvParser
async function fetchLiveStats() {
    try {
        const response = await fetch(liveStatsJsonPath + '?t=' + Date.now());
        if (!response.ok) {
            console.warn('Failed to fetch live stats:', response.status);
            return;
        }
        const liveStats = await response.json();
        updateJsonViewer(liveStats);
        processLiveStats(liveStats);
    } catch (error) {
        console.error('Error fetching live stats:', error);
    }
}

// Extract hostname from URL in request_name
function extractHostname(requestName) {
    if (!requestName) return null;
    
    try {
        // Check if it looks like a URL
        if (requestName.includes('://')) {
            const url = new URL(requestName);
            return url.hostname;
        }
        // Check if it starts with a method like "GET /path"
        const parts = requestName.split(' ');
        if (parts.length >= 2 && parts[1].includes('://')) {
            const url = new URL(parts[1]);
            return url.hostname;
        }
        // Try to extract hostname pattern from the string
        const hostnameMatch = requestName.match(/([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}/);
        if (hostnameMatch) {
            return hostnameMatch[0];
        }
    } catch (e) {
        // Not a valid URL, return null
    }
    return null;
}

// Populate filter dropdowns with unique values
function populateFilters(liveStats) {
    if (!liveStats || !liveStats.recent_requests) return;

    // Get unique scenarios, requests, and hostnames
    const scenarios = new Set();
    const requests = new Set();
    const hostnames = new Set();

    liveStats.recent_requests.forEach(req => {
        if (req.scenario) scenarios.add(req.scenario);
        if (req.request_name) requests.add(req.request_name);
        
        // Extract hostname from request_name
        const hostname = extractHostname(req.request_name);
        if (hostname) hostnames.add(hostname);
    });

    // Populate scenario dropdown
    const scenarioFilter = document.getElementById('scenarioFilter');
    if (scenarioFilter) {
        const currentValue = scenarioFilter.value || 'all';
        scenarioFilter.innerHTML = '<option value="all">📊 All Scenarios</option>';
        Array.from(scenarios).sort().forEach(scenario => {
            const option = document.createElement('option');
            option.value = scenario;
            option.textContent = scenario;
            scenarioFilter.appendChild(option);
        });
        scenarioFilter.value = currentValue;
    }

    // Populate request dropdown
    const requestFilter = document.getElementById('requestFilter');
    if (requestFilter) {
        const currentValue = requestFilter.value || 'all';
        requestFilter.innerHTML = '<option value="all">📋 All Requests</option>';
        Array.from(requests).sort().forEach(request => {
            const option = document.createElement('option');
            option.value = request;
            option.textContent = request;
            requestFilter.appendChild(option);
        });
        requestFilter.value = currentValue;
    }

    // Populate hostname dropdown
    const hostnameFilter = document.getElementById('hostnameFilter');
    if (hostnameFilter) {
        const currentValue = hostnameFilter.value || 'all';
        hostnameFilter.innerHTML = '<option value="all">🌐 All Hostnames</option>';
        Array.from(hostnames).sort().forEach(hostname => {
            const option = document.createElement('option');
            option.value = hostname;
            option.textContent = hostname;
            hostnameFilter.appendChild(option);
        });
        hostnameFilter.value = currentValue;
    }
}

// Filter requests based on selected scenario, request name, and hostname
function filterRequests(requests) {
    if (!requests) return [];

    return requests.filter(req => {
        const matchesScenario = selectedScenario === 'all' || req.scenario === selectedScenario;
        const matchesRequest = selectedRequest === 'all' || req.request_name === selectedRequest;
        
        // Check hostname match
        let matchesHostname = true;
        if (selectedHostname !== 'all') {
            const hostname = extractHostname(req.request_name);
            matchesHostname = hostname === selectedHostname;
        }
        
        return matchesScenario && matchesRequest && matchesHostname;
    });
}

// Handle scenario filter change
function handleScenarioFilterChange(value) {
    selectedScenario = value;
    if (rawLiveStatsData) {
        processLiveStats(rawLiveStatsData);
    }
}

// Handle request filter change
function handleRequestFilterChange(value) {
    selectedRequest = value;
    if (rawLiveStatsData) {
        processLiveStats(rawLiveStatsData);
    }
}

// Handle hostname filter change
function handleHostnameFilterChange(value) {
    selectedHostname = value;
    if (rawLiveStatsData) {
        processLiveStats(rawLiveStatsData);
    }
}

// Process LiveStats JSON and update dashboard
function processLiveStats(liveStats) {
    if (!liveStats) return;

    // Store raw data for filtering
    rawLiveStatsData = liveStats;

    // Populate filter dropdowns (only on first load or when data changes)
    populateFilters(liveStats);

    // Update header with scenario name
    const scenarioNameElement = document.getElementById('scenarioName');
    if (scenarioNameElement && liveStats.scenario) {
        scenarioNameElement.textContent = liveStats.scenario;
    }

    // Filter requests based on selected filters
    const filteredRequests = filterRequests(liveStats.recent_requests);

    // Recalculate summary stats for filtered data
    const filteredSummary = calculateFilteredSummary(filteredRequests);

    // Update metric cards from filtered summary
    if (filteredSummary && liveStats.vu_state) {
        updateMetricCards(filteredSummary, liveStats.vu_state);
    }

    // Update all charts with filtered requests data
    if (filteredRequests && filteredRequests.length > 0) {
        updateAllChartsFromRequests(filteredRequests);
    }

    // Update VU state charts with historical data
    if (liveStats.vu_state_history && liveStats.vu_state_history.length > 0) {
        updateVuStateCharts(liveStats.vu_state_history);
    } else if (liveStats.vu_state) {
        // Fallback to single state if no history available
        updateVuStateChart(liveStats.vu_state);
    }

    // Update data tables with filtered data
    updateDataTables(filteredRequests, liveStats.vu_state);
}

// Calculate summary statistics for filtered requests
function calculateFilteredSummary(requests) {
    if (!requests || requests.length === 0) {
        return {
            total_requests: 0,
            fail_count: 0,
            success_count: 0,
            success_rate: 0,
            avg_response_time: 0,
            p95: 0,
            throughput_rps: 0
        };
    }

    const failCount = requests.filter(r => r.status === 'Failed').length;
    const successCount = requests.length - failCount;
    const responseTimes = requests
        .filter(r => r.response_time_ms !== null)
        .map(r => r.response_time_ms)
        .sort((a, b) => a - b);

    const avgResponseTime = responseTimes.length > 0
        ? responseTimes.reduce((a, b) => a + b, 0) / responseTimes.length
        : 0;

    const p95Index = Math.floor(responseTimes.length * 0.95);
    const p95 = responseTimes.length > 0 ? responseTimes[p95Index] || 0 : 0;

    // Calculate throughput (requests per time window)
    const timestamps = requests.map(r => r.timestamp).filter(t => t);
    let throughput = 0;
    if (timestamps.length > 1) {
        const minTime = Math.min(...timestamps);
        const maxTime = Math.max(...timestamps);
        const durationSeconds = (maxTime - minTime) / 1000;
        throughput = durationSeconds > 0 ? requests.length / durationSeconds : 0;
    }

    return {
        total_requests: requests.length,
        fail_count: failCount,
        success_count: successCount,
        success_rate: requests.length > 0 ? successCount / requests.length : 0,
        avg_response_time: avgResponseTime,
        p95: p95,
        throughput_rps: throughput
    };
}

// Update metric cards
function updateMetricCards(summary, vuState) {
    const errorRate = summary.fail_count > 0 
        ? ((summary.fail_count / summary.total_requests) * 100).toFixed(2)
        : '0.00';
    document.getElementById('metricErrorRate').textContent = errorRate + '%';

    document.getElementById('metricTps').textContent = summary.throughput_rps.toFixed(2);
    document.getElementById('metricTotalRequests').textContent = summary.total_requests;
    document.getElementById('metricMaxVUsers').textContent = vuState.active;
    document.getElementById('metricP95').textContent = summary.p95.toFixed(2) + ' ms';

    // Duration - would need start_time in JSON to calculate properly
    document.getElementById('metricDuration').textContent = new Date().toLocaleTimeString();
}

// Update all charts from recent_requests array
function updateAllChartsFromRequests(requests) {
    if (!requests || requests.length === 0) return;

    // Group requests by time buckets (1 second intervals)
    const timeBuckets = {};
    const errorBuckets = {};
    const responseTimes = [];
    const statusCodes = {};

    requests.forEach(req => {
        const ts = Math.floor(req.timestamp / 1000) * 1000; // Round to second
        const timeLabel = new Date(ts).toLocaleTimeString();

        // Count requests per second
        timeBuckets[timeLabel] = (timeBuckets[timeLabel] || 0) + 1;

        // Count errors per second
        if (req.status === 'Failed') {
            errorBuckets[timeLabel] = (errorBuckets[timeLabel] || 0) + 1;
        }

        // Collect response times
        if (req.response_time_ms !== null) {
            responseTimes.push({
                time: timeLabel,
                value: req.response_time_ms
            });
        }

        // Count status codes (if you track HTTP status in CSV)
        const status = req.status || 'Unknown';
        statusCodes[status] = (statusCodes[status] || 0) + 1;
    });

    // Update request/response chart
    const timeLabels = Object.keys(timeBuckets).sort();
    const requestCounts = timeLabels.map(t => timeBuckets[t]);
    updateChartData(charts.reqRespChart, timeLabels, [requestCounts], ['Requests/s']);

    // Update errors chart
    const errorCounts = timeLabels.map(t => errorBuckets[t] || 0);
    updateChartData(charts.errorsChart, timeLabels, [errorCounts], ['Errors/s']);

    // Update response time chart (moving average)
    const rtAvg = calculateMovingAverage(responseTimes);
    updateChartData(charts.respTimeChart, rtAvg.labels, [rtAvg.values], ['Avg Response Time']);

    // Update status codes chart
    const statusLabels = Object.keys(statusCodes);
    const statusValues = statusLabels.map(s => statusCodes[s]);
    updateChartData(charts.respStatusChart, statusLabels, [statusValues], ['Count']);

    // Update connection timing charts if data available
    updateConnectionTimingCharts(requests);

    // Update bandwidth chart
    updateBandwidthChart(requests);

    // Update DNS resolutions chart
    updateDnsResolutionsChart(requests);
}

// Update bandwidth chart from bytes sent/received
function updateBandwidthChart(requests) {
    if (!requests || requests.length === 0) return;

    const bandwidthByTime = {};

    requests.forEach(req => {
        const ts = Math.floor(req.timestamp / 1000) * 1000;
        const timeLabel = new Date(ts).toLocaleTimeString();

        if (!bandwidthByTime[timeLabel]) {
            bandwidthByTime[timeLabel] = { sent: 0, received: 0 };
        }

        bandwidthByTime[timeLabel].sent += (req.bytes_sent || 0);
        bandwidthByTime[timeLabel].received += (req.bytes_received || 0);
    });

    const timeLabels = Object.keys(bandwidthByTime).sort();
    const sentMB = timeLabels.map(t => (bandwidthByTime[t].sent / (1024 * 1024)).toFixed(3));
    const receivedMB = timeLabels.map(t => (bandwidthByTime[t].received / (1024 * 1024)).toFixed(3));

    if (charts.bandwidthChart) {
        charts.bandwidthChart.data.labels = timeLabels;
        charts.bandwidthChart.data.datasets = [
            {
                label: 'Sent (MB/s)',
                data: sentMB,
                borderColor: getChartColor(0),
                backgroundColor: getChartColor(0, 0.2),
                borderWidth: 2,
                tension: 0.1,
                fill: false
            },
            {
                label: 'Received (MB/s)',
                data: receivedMB,
                borderColor: getChartColor(1),
                backgroundColor: getChartColor(1, 0.2),
                borderWidth: 2,
                tension: 0.1,
                fill: false
            }
        ];
        charts.bandwidthChart.update();
    }
}

// Update DNS resolutions chart
function updateDnsResolutionsChart(requests) {
    if (!requests || requests.length === 0) return;

    const dnsResolutionsByTime = {};

    requests.forEach(req => {
        if (req.dns_lookup_ms !== null && req.dns_lookup_ms !== undefined) {
            const ts = Math.floor(req.timestamp / 1000) * 1000;
            const timeLabel = new Date(ts).toLocaleTimeString();
            dnsResolutionsByTime[timeLabel] = (dnsResolutionsByTime[timeLabel] || 0) + 1;
        }
    });

    const timeLabels = Object.keys(dnsResolutionsByTime).sort();
    const resolutionCounts = timeLabels.map(t => dnsResolutionsByTime[t]);

    updateChartData(charts.dnsResChart, timeLabels, [resolutionCounts], ['DNS Resolutions/s']);
}

// Calculate moving average for response times
function calculateMovingAverage(dataPoints, windowSize = 10) {
    const grouped = {};
    dataPoints.forEach(dp => {
        if (!grouped[dp.time]) grouped[dp.time] = [];
        grouped[dp.time].push(dp.value);
    });

    const labels = Object.keys(grouped).sort();
    const values = labels.map(label => {
        const arr = grouped[label];
        return arr.reduce((a, b) => a + b, 0) / arr.length;
    });

    return { labels, values };
}

// Update connection timing charts (DNS, TCP, TLS, TTFB)
function updateConnectionTimingCharts(requests) {
    const timings = {
        dns: [],
        tcp: [],
        tls: [],
        ttfb: []
    };

    requests.forEach(req => {
        const timeLabel = new Date(req.timestamp).toLocaleTimeString();
        if (req.dns_lookup_ms !== null) {
            timings.dns.push({ time: timeLabel, value: req.dns_lookup_ms });
        }
        if (req.tcp_connect_ms !== null) {
            timings.tcp.push({ time: timeLabel, value: req.tcp_connect_ms });
        }
        if (req.tls_handshake_ms !== null) {
            timings.tls.push({ time: timeLabel, value: req.tls_handshake_ms });
        }
        if (req.time_to_first_byte_ms !== null) {
            timings.ttfb.push({ time: timeLabel, value: req.time_to_first_byte_ms });
        }
    });

    // Update DNS chart
    if (timings.dns.length > 0) {
        const dnsAvg = calculateMovingAverage(timings.dns);
        updateChartData(charts.dnsDurationChart, dnsAvg.labels, [dnsAvg.values], ['DNS Lookup (ms)']);
    }

    // Update TCP chart
    if (timings.tcp.length > 0) {
        const tcpAvg = calculateMovingAverage(timings.tcp);
        updateChartData(charts.tcpDurationChart, tcpAvg.labels, [tcpAvg.values], ['TCP Connect (ms)']);
    }

    // Update TLS chart
    if (timings.tls.length > 0) {
        const tlsAvg = calculateMovingAverage(timings.tls);
        updateChartData(charts.tlsChart, tlsAvg.labels, [tlsAvg.values], ['TLS Handshake (ms)']);
    }

    // Update connection open/close chart
    updateConnectionRateChart(requests);

    // Update TCP state chart
    updateTcpStateChart(requests);
}

// Update connection open/close rate chart
function updateConnectionRateChart(requests) {
    if (!requests || requests.length === 0) return;

    const connectionsByTime = {};

    requests.forEach(req => {
        // A new connection is implied when we have TCP connect time
        if (req.tcp_connect_ms !== null && req.tcp_connect_ms !== undefined) {
            const ts = Math.floor(req.timestamp / 1000) * 1000;
            const timeLabel = new Date(ts).toLocaleTimeString();
            connectionsByTime[timeLabel] = (connectionsByTime[timeLabel] || 0) + 1;
        }
    });

    const timeLabels = Object.keys(connectionsByTime).sort();
    const connCounts = timeLabels.map(t => connectionsByTime[t]);

    if (charts.connOpenCloseChart) {
        charts.connOpenCloseChart.data.labels = timeLabels;
        charts.connOpenCloseChart.data.datasets = [
            {
                label: 'New Connections/s',
                data: connCounts,
                borderColor: getChartColor(2),
                backgroundColor: getChartColor(2, 0.2),
                borderWidth: 2,
                tension: 0.1,
                fill: false
            }
        ];
        charts.connOpenCloseChart.update();
    }
}

// Update TCP state chart (categorize by timing characteristics)
function updateTcpStateChart(requests) {
    if (!requests || requests.length === 0) return;

    const tcpStates = {
        'Fast (<10ms)': 0,
        'Normal (10-50ms)': 0,
        'Slow (50-100ms)': 0,
        'Very Slow (>100ms)': 0,
        'No Data': 0
    };

    requests.forEach(req => {
        if (req.tcp_connect_ms === null || req.tcp_connect_ms === undefined) {
            tcpStates['No Data']++;
        } else if (req.tcp_connect_ms < 10) {
            tcpStates['Fast (<10ms)']++;
        } else if (req.tcp_connect_ms < 50) {
            tcpStates['Normal (10-50ms)']++;
        } else if (req.tcp_connect_ms < 100) {
            tcpStates['Slow (50-100ms)']++;
        } else {
            tcpStates['Very Slow (>100ms)']++;
        }
    });

    const stateLabels = Object.keys(tcpStates);
    const stateCounts = stateLabels.map(label => tcpStates[label]);

    if (charts.tcpStateChart) {
        charts.tcpStateChart.data.labels = stateLabels;
        charts.tcpStateChart.data.datasets = [
            {
                label: 'Connection Count',
                data: stateCounts,
                backgroundColor: [
                    'rgba(46, 204, 113, 0.6)',   // Green for fast
                    'rgba(52, 152, 219, 0.6)',   // Blue for normal
                    'rgba(241, 196, 15, 0.6)',   // Yellow for slow
                    'rgba(231, 76, 60, 0.6)',    // Red for very slow
                    'rgba(149, 165, 166, 0.6)'   // Gray for no data
                ],
                borderColor: [
                    'rgba(46, 204, 113, 1)',
                    'rgba(52, 152, 219, 1)',
                    'rgba(241, 196, 15, 1)',
                    'rgba(231, 76, 60, 1)',
                    'rgba(149, 165, 166, 1)'
                ],
                borderWidth: 1
            }
        ];
        charts.tcpStateChart.update();
    }
}

// Update VU state charts using historical data
function updateVuStateChart(vuState) {
    const timeLabel = new Date().toLocaleTimeString();
    
    // Update concurrent users chart with current state
    updateChartData(charts.concurrentChart, [timeLabel], [[vuState.active]], ['Active VUsers']);
}

// Update VU charts with historical data
function updateVuStateCharts(vuStateHistory) {
    if (!vuStateHistory || vuStateHistory.length === 0) return;

    const timeLabels = [];
    const activeUsers = [];
    const arrivalRates = [];
    const terminationRates = [];

    // Calculate rates between consecutive VU states
    for (let i = 0; i < vuStateHistory.length; i++) {
        const state = vuStateHistory[i];
        const timestamp = state.timestamp;
        const timeLabel = new Date(timestamp).toLocaleTimeString();
        timeLabels.push(timeLabel);
        activeUsers.push(state.active);

        if (i > 0) {
            const prevState = vuStateHistory[i - 1];
            
            // Calculate arrival rate (new VUs started)
            const newStarted = state.total_started - prevState.total_started;
            arrivalRates.push(newStarted);

            // Calculate termination rate (VUs completed + failed)
            const prevTerminated = prevState.completed + prevState.failed;
            const currTerminated = state.completed + state.failed;
            const newTerminated = currTerminated - prevTerminated;
            terminationRates.push(newTerminated);
        } else {
            // First data point - use 0 for rates
            arrivalRates.push(0);
            terminationRates.push(0);
        }
    }

    // Update concurrent users chart
    if (charts.concurrentChart) {
        updateChartData(charts.concurrentChart, timeLabels, [activeUsers], ['Active VUsers']);
    }

    // Update arrival chart
    if (charts.arrivalChart) {
        updateChartData(charts.arrivalChart, timeLabels, [arrivalRates], ['VUsers Started']);
    }

    // Update termination chart
    if (charts.terminationChart) {
        updateChartData(charts.terminationChart, timeLabels, [terminationRates], ['VUsers Terminated']);
    }
}

// Update data tables
function updateDataTables(requests, vuState) {
    // Update request/response table
    const reqRespTable = document.getElementById('reqRespTable');
    if (reqRespTable && requests && requests.length > 0) {
        // Show last 10 requests
        const recentRequests = requests.slice(-10);
        reqRespTable.innerHTML = recentRequests.map(req => `
            <tr>
                <td>${new Date(req.timestamp).toLocaleTimeString()}</td>
                <td>${req.request_name || 'N/A'}</td>
                <td>${req.status}</td>
                <td>${req.response_time_ms !== null ? req.response_time_ms.toFixed(2) : 'N/A'}</td>
                <td>${req.error_message || ''}</td>
            </tr>
        `).join('');
    }

    // Update users table
    const usersTable = document.getElementById('usersTable');
    if (usersTable && vuState) {
        usersTable.innerHTML = `
            <tr>
                <td>${new Date().toLocaleTimeString()}</td>
                <td>${vuState.active}</td>
                <td>${vuState.completed}</td>
                <td>${vuState.failed}</td>
                <td>${(vuState.success_rate * 100).toFixed(2)}%</td>
            </tr>
        `;
    }

    // Update connections table (from request data)
    const connTable = document.getElementById('connectionsTable');
    if (connTable && requests && requests.length > 0) {
        const recentConns = requests.slice(-10).filter(r => r.tcp_connect_ms !== null);
        connTable.innerHTML = recentConns.map(req => `
            <tr>
                <td>${new Date(req.timestamp).toLocaleTimeString()}</td>
                <td>${req.tcp_connect_ms !== null ? req.tcp_connect_ms.toFixed(2) : 'N/A'}</td>
                <td>${req.tls_handshake_ms !== null ? req.tls_handshake_ms.toFixed(2) : 'N/A'}</td>
            </tr>
        `).join('');
    }

    // Update DNS table
    const dnsTable = document.getElementById('dnsTable');
    if (dnsTable && requests && requests.length > 0) {
        const recentDns = requests.slice(-10).filter(r => r.dns_lookup_ms !== null);
        dnsTable.innerHTML = recentDns.map(req => `
            <tr>
                <td>${new Date(req.timestamp).toLocaleTimeString()}</td>
                <td>${req.request_name || 'N/A'}</td>
                <td>${req.dns_lookup_ms !== null ? req.dns_lookup_ms.toFixed(2) : 'N/A'}</td>
            </tr>
        `).join('');
    }
}

// Helper function to update chart data
function updateChartData(chart, labels, datasets, datasetLabels) {
    if (!chart) return;

    chart.data.labels = labels;
    chart.data.datasets = datasets.map((data, idx) => ({
        label: datasetLabels[idx] || `Dataset ${idx + 1}`,
        data: data,
        borderColor: getChartColor(idx),
        backgroundColor: getChartColor(idx, 0.2),
        tension: 0.1
    }));
    chart.update();
}

// Get chart color palette
function getChartColor(index, alpha = 1) {
    const colors = [
        `rgba(75, 192, 192, ${alpha})`,
        `rgba(255, 99, 132, ${alpha})`,
        `rgba(54, 162, 235, ${alpha})`,
        `rgba(255, 206, 86, ${alpha})`,
        `rgba(153, 102, 255, ${alpha})`,
        `rgba(255, 159, 64, ${alpha})`
    ];
    return colors[index % colors.length];
}

// Update JSON viewer (optional)
function updateJsonViewer(data) {
    const pre = document.getElementById('jsonViewerContent');
    pre.textContent += JSON.stringify(data) + "\n";
    if (document.getElementById('jsonAutoScroll').checked) {
        pre.scrollTop = pre.scrollHeight;
    }
}

// Periodically flush metrics to charts & tables
function startDashboardRefresh() {
    setInterval(() => {
        if (metricsBuffer.length === 0) return;
        metricsBuffer.forEach(updateChartsAndTables);
        metricsBuffer = [];
    }, refreshInterval);
}

// Update charts and tables for a single metric snapshot
function updateChartsAndTables(metric) {
    const tsVal = (metric.timestamp_ms !== undefined && metric.timestamp_ms !== null) ? metric.timestamp_ms : (metric.timestamp || Date.now());
    const t = new Date(tsVal).toLocaleTimeString();

    // Debug: print which data fields are being selected for each chart
    try {
        console.groupCollapsed(`Selected data for charts @ ${t}`);
        console.log('raw metric:', metric);
        const mapping = {
            reqRespChart: {
                used: metric.requests !== undefined || metric.responses !== undefined,
                fields: { requests: metric.requests, responses: metric.responses }
            },
            respStatusChart: {
                used: metric.status2xx !== undefined || metric.status4xx !== undefined || metric.status5xx !== undefined,
                fields: { status2xx: metric.status2xx, status4xx: metric.status4xx, status5xx: metric.status5xx }
            },
            respTimeChart: {
                used: metric.p50 !== undefined || metric.p95 !== undefined || metric.p99 !== undefined || (metric.latency && (metric.latency.p50 || metric.latency.p95 || metric.latency.p99)),
                fields: { p50: metric.p50, p95: metric.p95, p99: metric.p99, latency: metric.latency }
            },
            errorsChart: {
                used: metric.errors !== undefined || metric.errorsPerSec !== undefined,
                fields: { errors: metric.errors, errorsPerSec: metric.errorsPerSec }
            },
            arrivalChart: {
                used: metric.users && metric.users.arrivalRate !== undefined,
                fields: { arrivalRate: metric.users && metric.users.arrivalRate }
            },
            terminationChart: {
                used: metric.users && metric.users.terminationRate !== undefined,
                fields: { terminationRate: metric.users && metric.users.terminationRate }
            },
            concurrentChart: {
                used: metric.users && metric.users.concurrent !== undefined,
                fields: { concurrent: metric.users && metric.users.concurrent }
            },
            connections: {
                used: metric.connections !== undefined,
                fields: metric.connections
            },
            dns: {
                used: metric.dns !== undefined,
                fields: metric.dns
            }
        };
        console.log('mapping:', mapping);
        console.groupEnd();
    } catch (e) { console.warn('Failed to print selected data mapping', e); }

    // Metrics cards
    if (metric.errorRate !== undefined) document.getElementById('metricErrorRate').textContent = (metric.errorRate*100).toFixed(2) + "%";
    if (metric.tps !== undefined) document.getElementById('metricTps').textContent = metric.tps;
    if (metric.totalRequests !== undefined) document.getElementById('metricTotalRequests').textContent = metric.totalRequests;
    if (metric.maxVUsers !== undefined) document.getElementById('metricMaxVUsers').textContent = metric.maxVUsers;
    if (metric.p95 !== undefined) document.getElementById('metricP95').textContent = metric.p95 + "ms";
    if (metric.duration !== undefined) document.getElementById('metricDuration').textContent = metric.duration;

    // Requests/Responses table
    const reqTable = document.getElementById('reqRespTable');
    if (metric.requests !== undefined) {
        const row = `<tr>
            <td>${t}</td>
            <td>${metric.requests}</td>
            <td>${metric.responses}</td>
            <td>${metric.status2xx}</td>
            <td>${metric.status4xx}</td>
            <td>${metric.status5xx}</td>
        </tr>`;
        reqTable.insertAdjacentHTML('beforeend', row);
    }

    // Users table
    const usersTable = document.getElementById('usersTable');
    if (metric.users) {
        const row = `<tr>
            <td>${t}</td>
            <td>${metric.users.arrivalRate}</td>
            <td>${metric.users.terminationRate}</td>
            <td>${metric.users.concurrent}</td>
        </tr>`;
        usersTable.insertAdjacentHTML('beforeend', row);
    }

    // Connections table
    const connTable = document.getElementById('connectionsTable');
    if (metric.connections) {
        const row = `<tr>
            <td>${t}</td>
            <td>${metric.connections.openRate}</td>
            <td>${metric.connections.closeRate}</td>
            <td>${JSON.stringify(metric.connections.tcpStates)}</td>
            <td>${metric.connections.bandwidth}</td>
        </tr>`;
        connTable.insertAdjacentHTML('beforeend', row);
    }

    // DNS table
    const dnsTable = document.getElementById('dnsTable');
    if (metric.dns) {
        const row = `<tr>
            <td>${t}</td>
            <td>${metric.dns.resolutionsPerSec}</td>
            <td>${metric.dns.p50}</td>
            <td>${metric.dns.p95}</td>
            <td>${metric.dns.p99}</td>
        </tr>`;
        dnsTable.insertAdjacentHTML('beforeend', row);
    }

    // Update charts (example: reqRespChart)
    if (charts.reqRespChart) {
        const chart = charts.reqRespChart;
        if (!chart.data.labels) chart.data.labels = [];
        if (!chart.data.datasets[0].data) chart.data.datasets[0].data = [];
        if (metric.requests !== undefined) {
            chart.data.labels.push(t);
            chart.data.datasets[0].data.push(metric.requests);
            chart.update('none');
        } else {
            console.warn('reqRespChart: metric.requests is undefined for metric', metric);
        }
    }

    // Add similar updates for other charts as needed...
}

// Change refresh interval dynamically
function updateRefreshInterval(ms) {
    const newInterval = parseInt(ms);
    if (!newInterval || newInterval < 1000) {
        console.warn('Invalid refresh interval, using default 5000ms');
        refreshInterval = 5000;
        return;
    }
    
    refreshInterval = newInterval;
    
    // Clear existing interval
    if (fetchIntervalId) {
        clearInterval(fetchIntervalId);
    }
    
    // Restart with new interval
    fetchIntervalId = setInterval(fetchLiveStats, refreshInterval);
    
    console.log('Refresh interval updated to:', refreshInterval + 'ms');
}

// Chart view / table view toggle
function toggleView(view) {
    document.querySelectorAll('.chart-view').forEach(el => el.style.display = view === 'chart' ? 'block' : 'none');
    document.querySelectorAll('.table-view').forEach(el => el.style.display = view === 'table' ? 'block' : 'none');
}

// Modal for expanding charts
function expandChart(chartId, title) {
    const modal = document.getElementById('chartModal');
    const modalCanvas = document.getElementById('modalChart');
    const origChart = charts[chartId];
    if (!origChart) return;
    // Clone dataset
    const clonedData = JSON.parse(JSON.stringify(origChart.data));
    modalCanvas.getContext('2d').clearRect(0,0,modalCanvas.width, modalCanvas.height);
    new Chart(modalCanvas, {
        type: origChart.config.type,
        data: clonedData,
        options: {...origChart.config.options, plugins: {...origChart.config.options.plugins}}
    });
    document.getElementById('modalChartTitle').textContent = title;
    modal.style.display = 'block';
}

function closeModal() {
    document.getElementById('chartModal').style.display = 'none';
}

// Convert raw SSE metric to dashboard metric format
function convertRawMetricToDashboardMetric(raw) {
    return {
        timestamp_ms: raw.timestamp ? Date.parse(raw.timestamp) : Date.now(),
        requests: 1, // Each raw metric is one request
        responses: 1,
        status2xx: raw.status >= 200 && raw.status < 300 ? 1 : 0,
        status4xx: raw.status >= 400 && raw.status < 500 ? 1 : 0,
        status5xx: raw.status >= 500 && raw.status < 600 ? 1 : 0,
        p50: raw.latency,
        errors: raw.error ? 1 : 0,
        errorRate: raw.error ? 1 : 0,
        tps: 1,
        totalRequests: 1,
        maxVUsers: raw.vu_id || 0,
        duration: undefined,
        users: {
            arrivalRate: undefined,
            terminationRate: undefined,
            concurrent: undefined
        },
        connections: {
            openRate: undefined,
            closeRate: undefined,
            tcpStates: undefined,
            bandwidth: undefined
        },
        dns: {
            resolutionsPerSec: undefined,
            p50: raw.dns_ms || undefined,
            p95: undefined,
            p99: undefined
        }
    };
}

// Initialize everything
document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    
    // Attach event listeners to filter dropdowns
    const scenarioFilter = document.getElementById('scenarioFilter');
    if (scenarioFilter) {
        scenarioFilter.addEventListener('change', (e) => handleScenarioFilterChange(e.target.value));
    }
    
    const requestFilter = document.getElementById('requestFilter');
    if (requestFilter) {
        requestFilter.addEventListener('change', (e) => handleRequestFilterChange(e.target.value));
    }
    
    const hostnameFilter = document.getElementById('hostnameFilter');
    if (hostnameFilter) {
        hostnameFilter.addEventListener('change', (e) => handleHostnameFilterChange(e.target.value));
    }
    
    // Option 1: Use SSE for real-time streaming
    // connectSSE();
    // startDashboardRefresh();
    
    // Option 2: Use JSON file polling (fetch every refreshInterval)
    fetchLiveStats(); // Initial load
    fetchIntervalId = setInterval(fetchLiveStats, refreshInterval); // Periodic updates
});
