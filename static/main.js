// State management
const appState = {
    sheet1: {
        originalLoaded: false,
        cleaned: false,
        currentPage: { original: 1, included: 1, excluded: 1 },
        totalPages: { original: 1, included: 1, excluded: 1 },
        totalRecords: { original: 0, included: 0, excluded: 0 }
    },
    sheet2: {
        originalLoaded: false,
        cleaned: false,
        currentPage: { original: 1, included: 1, excluded: 1 },
        totalPages: { original: 1, included: 1, excluded: 1 },
        totalRecords: { original: 0, included: 0, excluded: 0 }
    }
};

// ==================== INITIALIZATION ====================

document.addEventListener('DOMContentLoaded', function() {
    console.log('Dashboard initializing...');
    setupEventListeners();
    
    // Check status for both sheets
    checkSheetStatus('sheet1');
    checkSheetStatus('sheet2');
});

function setupEventListeners() {
    // Load buttons
    document.querySelectorAll('[id$="-load-btn"]').forEach(btn => {
        btn.addEventListener('click', function() {
            const sheetKey = this.getAttribute('data-sheet');
            loadSheet(sheetKey);
        });
    });
    
    // Clean buttons
    document.querySelectorAll('[id$="-clean-btn"]').forEach(btn => {
        btn.addEventListener('click', function() {
            const sheetKey = this.getAttribute('data-sheet');
            cleanSheet(sheetKey);
        });
    });
    
    // Data tabs
    document.querySelectorAll('.data-tab').forEach(tab => {
        tab.addEventListener('click', function() {
            if (!this.classList.contains('disabled')) {
                const sheetKey = this.getAttribute('data-sheet');
                const viewType = this.getAttribute('data-view');
                switchDataView(sheetKey, viewType, this);
            }
        });
    });
    
    // Download buttons
    document.querySelectorAll('.download-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const sheetKey = this.getAttribute('data-sheet');
            const dataType = this.getAttribute('data-type');
            const format = this.getAttribute('data-format');
            downloadData(sheetKey, dataType, format);
        });
    });
}

// ==================== SHEET STATUS CHECK ====================

function checkSheetStatus(sheetKey) {
    fetch(`/api/check_cleaning_status/${sheetKey}`)
        .then(res => res.json())
        .then(data => {
            if (data.success) {
                const state = appState[sheetKey];
                
                // Update state
                state.originalLoaded = data.original_loaded;
                state.cleaned = data.cleaned;
                
                // Update UI
                const statusBadge = document.getElementById(`${sheetKey}-status`);
                const loadBtn = document.getElementById(`${sheetKey}-load-btn`);
                const cleanBtn = document.getElementById(`${sheetKey}-clean-btn`);
                
                if (data.original_loaded) {
                    statusBadge.textContent = 'Data Loaded';
                    statusBadge.className = 'status-badge loaded';
                    loadBtn.textContent = '✓ Data Loaded';
                    loadBtn.classList.add('success');
                    loadBtn.disabled = true;
                    cleanBtn.disabled = false;
                    
                    // Load original data
                    loadOriginalData(sheetKey, 1);
                }
                
                if (data.cleaned) {
                    statusBadge.textContent = 'Cleaned';
                    statusBadge.className = 'status-badge cleaned';
                    cleanBtn.textContent = '✓ Data Cleaned';
                    cleanBtn.classList.add('success');
                    cleanBtn.disabled = true;
                    
                    // Enable tabs
                    const includedTab = document.getElementById(`${sheetKey}-included-tab`);
                    const excludedTab = document.getElementById(`${sheetKey}-excluded-tab`);
                    const analyticsTab = document.getElementById(`${sheetKey}-analytics-tab`);
                    
                    includedTab.classList.remove('disabled');
                    excludedTab.classList.remove('disabled');
                    analyticsTab.classList.remove('disabled');
                    
                    // Update counts in tabs
                    includedTab.innerHTML = `Included Rows <span style="background: rgba(255,255,255,0.3); padding: 2px 8px; border-radius: 10px; margin-left: 5px;">${data.included_count}</span>`;
                    excludedTab.innerHTML = `Excluded Rows <span style="background: rgba(255,255,255,0.3); padding: 2px 8px; border-radius: 10px; margin-left: 5px;">${data.excluded_count}</span>`;
                    
                    // Store counts
                    state.totalRecords.included = data.included_count;
                    state.totalRecords.excluded = data.excluded_count;
                    
                    // Load cleaned data
                    loadCleanedData(sheetKey, 'included', 1);
                    loadCleanedData(sheetKey, 'excluded', 1);
                    
                    // Render analytics
                    if (data.analytics) {
                        renderAnalytics(sheetKey, data.analytics);
                    }
                }
            }
        })
        .catch(error => {
            console.error(`Error checking status for ${sheetKey}:`, error);
        });
}

// ==================== LOAD SHEET ====================

function loadSheet(sheetKey) {
    const loadBtn = document.getElementById(`${sheetKey}-load-btn`);
    const statusBadge = document.getElementById(`${sheetKey}-status`);
    const originalTab = document.getElementById(`${sheetKey}-original-tab`);
    
    loadBtn.disabled = true;
    loadBtn.textContent = '⏳ Loading...';
    statusBadge.textContent = 'Loading...';
    statusBadge.className = 'status-badge pending';
    
    fetch(`/api/load_sheet/${sheetKey}`)
        .then(res => res.json())
        .then(data => {
            if (data.success) {
                appState[sheetKey].originalLoaded = true;
                appState[sheetKey].totalRecords.original = data.row_count;
                
                statusBadge.textContent = 'Data Loaded';
                statusBadge.className = 'status-badge loaded';
                loadBtn.textContent = '✓ Data Loaded';
                loadBtn.classList.add('success');
                
                // Update original tab with count
                originalTab.innerHTML = `Original Data <span style="background: rgba(255,255,255,0.3); padding: 2px 8px; border-radius: 10px; margin-left: 5px;">${data.row_count.toLocaleString()}</span>`;
                
                // Enable clean button
                const cleanBtn = document.getElementById(`${sheetKey}-clean-btn`);
                cleanBtn.disabled = false;
                
                // Load original data to display
                loadOriginalData(sheetKey, 1);
                
            } else {
                throw new Error(data.error || 'Failed to load sheet');
            }
        })
        .catch(error => {
            console.error('Error loading sheet:', error);
            alert('Error loading sheet: ' + error.message);
            
            loadBtn.disabled = false;
            loadBtn.textContent = '📥 Load Data';
            statusBadge.textContent = 'Error';
            statusBadge.className = 'status-badge pending';
        });
}

// ==================== CLEAN SHEET ====================

function cleanSheet(sheetKey) {
    const cleanBtn = document.getElementById(`${sheetKey}-clean-btn`);
    const statusBadge = document.getElementById(`${sheetKey}-status`);
    
    cleanBtn.disabled = true;
    cleanBtn.textContent = '⏳ Cleaning...';
    statusBadge.textContent = 'Cleaning...';
    
    fetch('/api/clean_data', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sheet: sheetKey })
    })
    .then(res => res.json())
    .then(data => {
        if (data.success) {
            appState[sheetKey].cleaned = true;
            
            statusBadge.textContent = 'Cleaned';
            statusBadge.className = 'status-badge cleaned';
            cleanBtn.textContent = '✓ Data Cleaned';
            cleanBtn.classList.add('success');
            
            // Enable tabs
            const includedTab = document.getElementById(`${sheetKey}-included-tab`);
            const excludedTab = document.getElementById(`${sheetKey}-excluded-tab`);
            const analyticsTab = document.getElementById(`${sheetKey}-analytics-tab`);
            
            includedTab.classList.remove('disabled');
            excludedTab.classList.remove('disabled');
            analyticsTab.classList.remove('disabled');
            
            // Update counts
            const summary = data.summary;
            includedTab.innerHTML = `Included Rows <span style="background: rgba(255,255,255,0.3); padding: 2px 8px; border-radius: 10px; margin-left: 5px;">${summary.included_count}</span>`;
            excludedTab.innerHTML = `Excluded Rows <span style="background: rgba(255,255,255,0.3); padding: 2px 8px; border-radius: 10px; margin-left: 5px;">${summary.excluded_count}</span>`;
            
            // Load cleaned data
            loadCleanedData(sheetKey, 'included', 1);
            loadCleanedData(sheetKey, 'excluded', 1);
            loadAnalytics(sheetKey);
            
            alert('Data cleaned successfully!');
        } else {
            throw new Error(data.error || 'Failed to clean data');
        }
    })
    .catch(error => {
        console.error('Error cleaning data:', error);
        alert('Error cleaning data: ' + error.message);
        
        cleanBtn.disabled = false;
        cleanBtn.textContent = '🧹 Clean Data';
    });
}

// ==================== VIEW SWITCHING ====================

function switchDataView(sheetKey, viewType, clickedTab) {
    const container = document.getElementById(`${sheetKey}-container`);
    
    // Update tabs
    container.querySelectorAll('.data-tab').forEach(tab => tab.classList.remove('active'));
    clickedTab.classList.add('active');
    
    // Update views
    container.querySelectorAll('.data-view').forEach(view => view.classList.remove('active'));
    document.getElementById(`${sheetKey}-${viewType}`).classList.add('active');
    
    // Load analytics if switching to analytics view
    if (viewType === 'analytics') {
        const analyticsContent = document.getElementById(`${sheetKey}-analytics-content`);
        if (!analyticsContent.innerHTML || analyticsContent.innerHTML.trim() === '') {
            analyticsContent.innerHTML = '<div class="loading-overlay active"><div class="spinner"></div><p>Loading analytics...</p></div>';
            loadAnalytics(sheetKey);
        }
    }
}
// ==================== DATA LOADING ====================

function loadOriginalData(sheetKey, page) {
    const container = document.getElementById(`${sheetKey}-original`);
    const state = appState[sheetKey];
    
    // Show loading
    container.innerHTML = '<div class="loading-overlay active"><div class="spinner"></div><p>Loading original data...</p></div>';
    
    const perPage = 100;
    
    // First, check if original data exists in Supabase
    fetch(`/api/check_original_in_supabase/${sheetKey}`)
        .then(res => res.json())
        .then(checkData => {
            if (checkData.exists) {
                // Load from Supabase (stored original data)
                console.log(`Loading original data from Supabase for ${sheetKey}`);
                
                fetch(`/api/get_original_data_from_supabase/${sheetKey}?page=${page}&per_page=${perPage}`)
                    .then(res => res.json())
                    .then(data => {
                        if (data.success) {
                            state.totalRecords.original = data.total;
                            const totalPages = Math.ceil(data.total / perPage);
                            const columns = ['original_row_number', 'firstname', 'birthday', 'birthmonth', 'birthyear'];
                            renderTable(container, data.data, columns, sheetKey, 'original', page, totalPages, data.total);
                        } else {
                            container.innerHTML = '<p style="padding: 20px; color: #dc3545;">Error loading original data</p>';
                        }
                    })
                    .catch(error => {
                        console.error('Error loading original data from Supabase:', error);
                        container.innerHTML = '<p style="padding: 20px; color: #dc3545;">Error loading original data</p>';
                    });
            } else {
                // Load from Google Sheets (no stored original)
                console.log(`Loading original data from Google Sheets for ${sheetKey}`);
                
                fetch(`/api/get_original_data/${sheetKey}?page=${page}&per_page=${perPage}`)
                    .then(res => res.json())
                    .then(data => {
                        if (data.success) {
                            state.totalRecords.original = data.total_records;
                            const columns = ['original_row_number', 'firstname', 'birthday', 'birthmonth', 'birthyear'];
                            renderTable(container, data.data, columns, sheetKey, 'original', page, data.total_pages, data.total_records);
                        } else {
                            container.innerHTML = '<p style="padding: 20px; color: #dc3545;">Error loading original data</p>';
                        }
                    })
                    .catch(error => {
                        console.error('Error loading original data from Google Sheets:', error);
                        container.innerHTML = '<p style="padding: 20px; color: #dc3545;">Error loading original data</p>';
                    });
            }
        })
        .catch(error => {
            console.error('Error checking original data source:', error);
            container.innerHTML = '<p style="padding: 20px; color: #dc3545;">Error checking data source</p>';
        });
}

function loadCleanedData(sheetKey, dataType, page) {
    const contentDiv = document.getElementById(`${sheetKey}-${dataType}-content`);
    contentDiv.innerHTML = '<div class="loading-overlay active"><div class="spinner"></div></div>';
    
    fetch(`/api/get_cleaned_data/${sheetKey}?page=${page}&per_page=100&type=${dataType}`)
        .then(res => res.json())
        .then(data => {
            if (data.success) {
                appState[sheetKey].currentPage[dataType] = page;
                appState[sheetKey].totalPages[dataType] = data.total_pages;
                appState[sheetKey].totalRecords[dataType] = data.total_records;
                
                const columns = dataType === 'included' 
                    ? ['row_id', 'name', 'birth_day', 'birth_month', 'birth_year']
                    : ['row_id', 'original_name', 'original_birth_day', 'original_birth_month', 'original_birth_year', 'exclusion_reason'];
                
                renderTable(contentDiv, data.data, columns, sheetKey, dataType, page, data.total_pages, data.total_records);
                
                // Show download buttons
                document.getElementById(`${sheetKey}-${dataType}-downloads`).style.display = 'flex';
            }
        })
        .catch(error => {
            console.error('Error loading cleaned data:', error);
            contentDiv.innerHTML = '<p style="padding: 20px; text-align: center; color: #dc3545;">Error loading data</p>';
        });
}

function loadAnalytics(sheetKey) {
    fetch(`/api/get_analytics/${sheetKey}`)
        .then(res => res.json())
        .then(data => {
            if (data.success) {
                renderAnalytics(sheetKey, data.analytics);
            }
        })
        .catch(error => {
            console.error('Error loading analytics:', error);
        });
}

// ==================== RENDERING ====================
function renderTable(container, data, columns, sheetKey, tableType, currentPage, totalPages, totalRecords) {
    if (!data || data.length === 0) {
        container.innerHTML = '<p style="padding: 20px; text-align: center; color: #6c757d;">No data available</p>';
        return;
    }
    
    let tableHTML = `
        <div class="table-container">
            <table class="data-table">
                <thead>
                    <tr>
    `;
    
    // Different headers based on table type
    if (tableType === 'original') {
        tableHTML += `
            <th>Row Number</th>
            <th>Row ID</th>
            <th>First Name</th>
            <th>Birth Day</th>
            <th>Birth Month</th>
            <th>Birth Year</th>
        `;
    } else if (tableType === 'included') {
        tableHTML += `
            <th>Row Number</th>
            <th>Row ID</th>
            <th>Name</th>
            <th>Birth Day</th>
            <th>Birth Month</th>
            <th>Birth Year</th>
        `;
    } else if (tableType === 'excluded') {
        tableHTML += `
            <th>Row Number</th>
            <th>Row ID</th>
            <th>Original Name</th>
            <th>Original Birth Day</th>
            <th>Original Birth Month</th>
            <th>Original Birth Year</th>
            <th>Exclusion Reason</th>
        `;
    }
    
    tableHTML += `
                    </tr>
                </thead>
                <tbody>
    `;
    
    // Render rows based on type
    data.forEach(row => {
        if (tableType === 'original') {
            tableHTML += `
                <tr>
                    <td>${row.original_row_number || ''}</td>
                    <td style="font-family: monospace; font-size: 0.85em;">${row.row_id || ''}</td>
                    <td>${row.firstname || ''}</td>
                    <td>${row.birthday || ''}</td>
                    <td>${row.birthmonth || ''}</td>
                    <td>${row.birthyear || ''}</td>
                </tr>
            `;
        } else if (tableType === 'included') {
            tableHTML += `
                <tr>
                    <td>${row.original_row_number || ''}</td>
                    <td style="font-family: monospace; font-size: 0.85em;">${row.row_id || ''}</td>
                    <td>${row.name || ''}</td>
                    <td>${row.birth_day || ''}</td>
                    <td>${row.birth_month || ''}</td>
                    <td>${row.birth_year || ''}</td>
                </tr>
            `;
        } else if (tableType === 'excluded') {
            tableHTML += `
                <tr>
                    <td>${row.original_row_number || ''}</td>
                    <td style="font-family: monospace; font-size: 0.85em;">${row.row_id || ''}</td>
                    <td>${row.original_name || ''}</td>
                    <td>${row.original_birth_day || ''}</td>
                    <td>${row.original_birth_month || ''}</td>
                    <td>${row.original_birth_year || ''}</td>
                    <td>${row.exclusion_reason || ''}</td>
                </tr>
            `;
        }
    });
    
    tableHTML += `
                </tbody>
            </table>
        </div>
        <div class="pagination">
    `;
    
    // Different pagination functions based on type
    if (tableType === 'original') {
        tableHTML += `
            <button onclick="loadOriginalData('${sheetKey}', ${currentPage - 1})" 
                    ${currentPage === 1 ? 'disabled' : ''}>
                Previous
            </button>
            <span>Page ${currentPage} of ${totalPages} (${totalRecords.toLocaleString()} total records)</span>
            <button onclick="loadOriginalData('${sheetKey}', ${currentPage + 1})" 
                    ${currentPage === totalPages ? 'disabled' : ''}>
                Next
            </button>
        `;
    } else if (tableType === 'included') {
        tableHTML += `
            <button onclick="loadCleanedData('${sheetKey}', 'included', ${currentPage - 1})" 
                    ${currentPage === 1 ? 'disabled' : ''}>
                Previous
            </button>
            <span>Page ${currentPage} of ${totalPages} (${totalRecords.toLocaleString()} total records)</span>
            <button onclick="loadCleanedData('${sheetKey}', 'included', ${currentPage + 1})" 
                    ${currentPage === totalPages ? 'disabled' : ''}>
                Next
            </button>
        `;
    } else if (tableType === 'excluded') {
        tableHTML += `
            <button onclick="loadCleanedData('${sheetKey}', 'excluded', ${currentPage - 1})" 
                    ${currentPage === 1 ? 'disabled' : ''}>
                Previous
            </button>
            <span>Page ${currentPage} of ${totalPages} (${totalRecords.toLocaleString()} total records)</span>
            <button onclick="loadCleanedData('${sheetKey}', 'excluded', ${currentPage + 1})" 
                    ${currentPage === totalPages ? 'disabled' : ''}>
                Next
            </button>
        `;
    }
    
    tableHTML += `
        </div>
    `;
    
    container.innerHTML = tableHTML;
}

function loadPageData(sheetKey, tableType, page) {
    if (tableType === 'original') {
        loadOriginalData(sheetKey, page);
    } else {
        loadCleanedData(sheetKey, tableType, page);
    }
}

function renderAnalytics(sheetKey, analytics) {
    const container = document.getElementById(`${sheetKey}-analytics-content`);
    
    let html = '<div class="analytics-container">';
    
    // Dataset Overview
    html += '<div class="analytics-grid">';
    html += '<div class="analytics-card">';
    html += '<h3>📊 Dataset Overview</h3>';
    html += `<div class="stat-item"><span class="stat-label">Original Rows</span><span class="stat-value">${analytics.dataset_sizes.original_row_count.toLocaleString()}</span></div>`;
    html += `<div class="stat-item"><span class="stat-label">Included Rows</span><span class="stat-value" style="color: #28a745;">${analytics.dataset_sizes.included_row_count.toLocaleString()} (${analytics.dataset_sizes.percent_included_vs_original}%)</span></div>`;
    html += `<div class="stat-item"><span class="stat-label">Excluded Rows</span><span class="stat-value" style="color: #dc3545;">${analytics.dataset_sizes.excluded_row_count.toLocaleString()} (${analytics.dataset_sizes.percent_excluded_vs_original}%)</span></div>`;
    html += '</div>';
    
    // Uniqueness Metrics
    html += '<div class="analytics-card">';
    html += '<h3>🔍 Uniqueness Metrics</h3>';
    html += `<div class="stat-item"><span class="stat-label">Unique Names</span><span class="stat-value">${analytics.uniqueness_metrics.unique_names.toLocaleString()}</span></div>`;
    html += `<div class="stat-item"><span class="stat-label">Unique Birthdays</span><span class="stat-value">${analytics.uniqueness_metrics.unique_birthday_combinations.toLocaleString()}</span></div>`;
    html += `<div class="stat-item"><span class="stat-label">Name+Year Combos</span><span class="stat-value">${analytics.uniqueness_metrics.unique_name_year.toLocaleString()}</span></div>`;
    html += '</div>';
    html += '</div>';
    
    // Birth Year Distribution
    if (analytics.birth_year_distribution && analytics.birth_year_distribution.length > 0) {
        html += '<div class="analytics-card" style="margin-bottom: 20px;">';
        html += '<h3>📅 Birth Year Distribution</h3>';
        html += '<div style="max-height: 300px; overflow-y: auto;"><table style="width: 100%;"><thead><tr><th>Year</th><th>Count</th></tr></thead><tbody>';
        analytics.birth_year_distribution.forEach(item => {
            html += `<tr><td>${item.year}</td><td>${item.count.toLocaleString()}</td></tr>`;
        });
        html += '</tbody></table></div></div>';
    }
    
    // Exclusion Reasons
    if (analytics.exclusion_reasons && analytics.exclusion_reasons.length > 0) {
        html += '<div class="analytics-card">';
        html += '<h3>❌ Exclusion Reasons</h3>';
        html += '<table style="width: 100%;"><thead><tr><th>Reason</th><th>Count</th></tr></thead><tbody>';
        analytics.exclusion_reasons.forEach(item => {
            html += `<tr><td>${item.reason}</td><td>${item.count.toLocaleString()}</td></tr>`;
        });
        html += '</tbody></table></div>';
    }
    
    html += '</div>';
    container.innerHTML = html;
}

// ==================== DOWNLOADS ====================

function downloadData(sheetKey, dataType, format) {
    const url = `/api/download/${dataType}_${format}/${sheetKey}`;
    window.location.href = url;
}