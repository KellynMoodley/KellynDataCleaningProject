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
                
                // Enable clean button
                const cleanBtn = document.getElementById(`${sheetKey}-clean-btn`);
                cleanBtn.disabled = false;
                
                // Load original data to display
                loadOriginalData(sheetKey, 1);
                
                alert(`Successfully loaded ${data.row_count.toLocaleString()} rows!`);
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
}

// ==================== DATA LOADING ====================

function loadOriginalData(sheetKey, page) {
    const contentDiv = document.getElementById(`${sheetKey}-original`);
    contentDiv.innerHTML = '<div class="loading-overlay active"><div class="spinner"></div></div>';
    
    fetch(`/api/get_original_data/${sheetKey}?page=${page}&per_page=100`)
        .then(res => res.json())
        .then(data => {
            if (data.success) {
                appState[sheetKey].currentPage.original = page;
                appState[sheetKey].totalPages.original = data.total_pages;
                appState[sheetKey].totalRecords.original = data.total_records;


                const columns = ['original_row_number', 'row_id', 'firstname', 'birthday', 'birthmonth', 'birthyear'];


                renderTable(contentDiv, data.data, columns, 
                           sheetKey, 'original', page, data.total_pages, data.total_records);
            }
        })
        .catch(error => {
            console.error('Error loading original data:', error);
            contentDiv.innerHTML = '<p style="padding: 20px; text-align: center; color: #dc3545;">Error loading data</p>';
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
    
    // Column display name mapping
    const columnDisplayNames = {
        'row_id': 'Row ID',
        'original_row_number': 'Row Number',
        'firstname': 'First Name',
        'birthday': 'Birth Day',
        'birthmonth': 'Birth Month',
        'birthyear': 'Birth Year',
        'name': 'Name',
        'birth_day': 'Birth Day',
        'birth_month': 'Birth Month',
        'birth_year': 'Birth Year',
        'original_name': 'Original Name',
        'original_birth_day': 'Original Birth Day',
        'original_birth_month': 'Original Birth Month',
        'original_birth_year': 'Original Birth Year',
        'exclusion_reason': 'Exclusion Reason'
    };
    
    let html = '<div class="table-wrapper"><table><thead><tr>';
    columns.forEach(col => {
        const displayName = columnDisplayNames[col] || col.replace(/_/g, ' ').toUpperCase();
        html += `<th>${displayName}</th>`;
    });
    html += '</tr></thead><tbody>';
    
    data.forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            let value = row[col];
            if (col === 'row_id') {
                html += `<td style="font-family: monospace; font-size: 0.85em;">${value || '-'}</td>`;
            } else{
            html += `<td>${value || '-'}</td>`;
            }
        });
        html += '</tr>';
    });
    
    html += '</tbody></table>';
    
    // Add pagination
    if (totalPages > 1) {
        html += '<div class="pagination">';
        html += `<span class="pagination-info">Page ${currentPage} of ${totalPages} (${totalRecords.toLocaleString()} total rows)</span>`;
        html += '<div class="pagination-controls">';
        
        // Previous
        if (currentPage > 1) {
            html += `<button onclick="loadPageData('${sheetKey}', '${tableType}', ${currentPage - 1})">‹ Previous</button>`;
        } else {
            html += `<button disabled>‹ Previous</button>`;
        }
        
        // Page numbers
        const maxButtons = 5;
        let startPage = Math.max(1, currentPage - Math.floor(maxButtons / 2));
        let endPage = Math.min(totalPages, startPage + maxButtons - 1);
        
        if (endPage - startPage < maxButtons - 1) {
            startPage = Math.max(1, endPage - maxButtons + 1);
        }
        
        if (startPage > 1) {
            html += '<span>...</span>';
        }
        
        for (let i = startPage; i <= endPage; i++) {
            if (i === currentPage) {
                html += `<button class="active">${i}</button>`;
            } else {
                html += `<button onclick="loadPageData('${sheetKey}', '${tableType}', ${i})">${i}</button>`;
            }
        }
        
        if (endPage < totalPages) {
            html += '<span>...</span>';
        }
        
        // Next
        if (currentPage < totalPages) {
            html += `<button onclick="loadPageData('${sheetKey}', '${tableType}', ${currentPage + 1})">Next ›</button>`;
        } else {
            html += `<button disabled>Next ›</button>`;
        }
        
        html += '</div></div>';
    }
    
    html += '</div>';
    container.innerHTML = html;
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