let currentPage = 0;
let totalPages = 0;
let currentSheet = '';
let currentTab = null;
let isDataLoaded = false;
let totalRows = 0;

// Pagination state for cleaned data
let cleanedDataPagination = {
    sheet1: { included: 1, excluded: 1 },
    sheet2: { included: 1, excluded: 1 }
};

// ==================== INITIALIZATION ====================

document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM loaded, setting up...');
    setupEventListeners();
    
    // Load first sheet
    const firstTab = document.querySelector('.tab[data-sheet="sheet1"]');
    if (firstTab) {
        console.log('Loading first sheet...');
        loadTable('sheet1', firstTab);
    }
    
    // Load original data for cleaning view
    loadOriginalDataForCleaning('sheet1');
    loadOriginalDataForCleaning('sheet2');
    
    // Check for existing cleaned data
    checkExistingCleanedData('sheet1');
    checkExistingCleanedData('sheet2');
});

function setupEventListeners() {
    console.log('Setting up event listeners...');
    
    // Main view tabs
    document.querySelectorAll('.main-tab').forEach(tab => {
        tab.addEventListener('click', function() {
            const viewId = this.getAttribute('data-view');
            console.log('Main tab clicked:', viewId);
            switchMainView(viewId, this);
        });
    });
    
    // Sheet tabs (View Data section)
    document.querySelectorAll('.tabs .tab').forEach(tab => {
        tab.addEventListener('click', function() {
            const sheet = this.getAttribute('data-sheet');
            console.log('Sheet tab clicked:', sheet);
            loadTable(sheet, this);
        });
    });
    
    // Data view tabs (Clean Data section)
    document.querySelectorAll('.data-tab').forEach(tab => {
        tab.addEventListener('click', function() {
            if (!this.classList.contains('disabled')) {
                const sheetKey = this.getAttribute('data-sheet');
                const viewType = this.getAttribute('data-view');
                console.log('Data tab clicked:', sheetKey, viewType);
                switchDataView(sheetKey, viewType, this);
            }
        });
    });
    
    // Clean buttons
    document.querySelectorAll('.sheet-clean-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const sheetKey = this.getAttribute('data-sheet');
            console.log('Clean button clicked:', sheetKey);
            cleanSheetData(sheetKey, this);
        });
    });
    
    // Download buttons
    document.querySelectorAll('.download-btn').forEach(btn => {
        btn.addEventListener('click', function() {
            const sheetKey = this.getAttribute('data-sheet');
            const dataType = this.getAttribute('data-type');
            const format = this.getAttribute('data-format');
            console.log('Download button clicked:', sheetKey, dataType, format);
            downloadData(sheetKey, dataType, format);
        });
    });
    
    console.log('Event listeners setup complete');
}

// ==================== CHECK FOR EXISTING DATA ====================

function checkExistingCleanedData(sheetKey) {
    fetch(`/api/check_existing_data/${sheetKey}`)
        .then(res => res.json())
        .then(data => {
            if (data.exists) {
                console.log(`Found existing cleaned data for ${sheetKey}`);
                
                // Enable tabs and add counts
                const includedTab = document.getElementById(`${sheetKey}-included-tab`);
                const excludedTab = document.getElementById(`${sheetKey}-excluded-tab`);
                
                includedTab.classList.remove('disabled');
                excludedTab.classList.remove('disabled');
                
                includedTab.innerHTML = `Included Rows <span style="background: rgba(255,255,255,0.3); padding: 2px 8px; border-radius: 10px; margin-left: 5px;">${data.included_count}</span>`;
                excludedTab.innerHTML = `Excluded Rows <span style="background: rgba(255,255,255,0.3); padding: 2px 8px; border-radius: 10px; margin-left: 5px;">${data.excluded_count}</span>`;
                
                // Load the data with pagination
                loadCleanedDataWithPagination(sheetKey, 'included', 1);
                loadCleanedDataWithPagination(sheetKey, 'excluded', 1);
            }
        })
        .catch(error => {
            console.log(`No existing data found for ${sheetKey}:`, error);
        });
}

// ==================== MAIN VIEW SWITCHING ====================

function switchMainView(viewId, clickedTab) {
    console.log('Switching to view:', viewId);
    document.querySelectorAll('.main-tab').forEach(tab => tab.classList.remove('active'));
    clickedTab.classList.add('active');
    
    document.querySelectorAll('.view-section').forEach(section => section.classList.remove('active'));
    document.getElementById(viewId).classList.add('active');
}

// ==================== DOWNLOAD FUNCTIONS ====================

function downloadData(sheetKey, dataType, fileType) {
    const url = `/api/download/${dataType}_${fileType}/${sheetKey}`;
    
    // Create a temporary link and trigger download
    const link = document.createElement('a');
    link.href = url;
    link.download = `${sheetKey}_${dataType}_data.${fileType}`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

function showDownloadButtons(sheetKey, dataType) {
    const downloadDiv = document.getElementById(`${sheetKey}-${dataType}-downloads`);
    if (downloadDiv) {
        downloadDiv.style.display = 'flex';
    }
}

// ==================== VIEW DATA FUNCTIONS ====================

function updateStatusBadge(status, message) {
    const badge = document.getElementById('status-badge');
    if (badge) {
        badge.className = 'status-badge ' + status;
        badge.textContent = message;
    }
}

function updateRowCount(current, total) {
    const elem = document.getElementById('row-count');
    if (elem) {
        elem.textContent = `Rows: ${current.toLocaleString()} / ${total.toLocaleString()}`;
    }
}

function updatePageInfo(current, total) {
    const elem = document.getElementById('page-info');
    if (elem) {
        elem.textContent = `Page: ${current + 1} / ${total}`;
    }
}

function showLoadingOverlay() {
    const overlay = document.getElementById('loading-overlay');
    if (overlay) {
        overlay.classList.add('active');
    }
}

function hideLoadingOverlay() {
    const overlay = document.getElementById('loading-overlay');
    if (overlay) {
        overlay.classList.remove('active');
    }
}

function renderPageButtons() {
    const paginationDiv = document.getElementById('pagination');
    if (!paginationDiv) return;
    
    paginationDiv.innerHTML = '';

    if (totalPages === 0) return;

    const prevBtn = document.createElement('button');
    prevBtn.innerHTML = '← Prev';
    prevBtn.disabled = currentPage === 0;
    prevBtn.onclick = () => loadPage(currentPage - 1);
    paginationDiv.appendChild(prevBtn);

    const maxButtons = 7;
    let startPage = Math.max(0, currentPage - Math.floor(maxButtons / 2));
    let endPage = Math.min(totalPages, startPage + maxButtons);
    
    if (endPage - startPage < maxButtons) {
        startPage = Math.max(0, endPage - maxButtons);
    }

    if (startPage > 0) {
        const firstBtn = document.createElement('button');
        firstBtn.innerText = '1';
        firstBtn.onclick = () => loadPage(0);
        paginationDiv.appendChild(firstBtn);
        
        if (startPage > 1) {
            const ellipsis = document.createElement('span');
            ellipsis.innerHTML = '...';
            ellipsis.style.padding = '10px';
            paginationDiv.appendChild(ellipsis);
        }
    }

    for(let i = startPage; i < endPage; i++) {
        const btn = document.createElement('button');
        btn.innerText = i + 1;
        btn.className = i === currentPage ? 'active' : '';
        btn.onclick = () => loadPage(i);
        paginationDiv.appendChild(btn);
    }

    if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
            const ellipsis = document.createElement('span');
            ellipsis.innerHTML = '...';
            ellipsis.style.padding = '10px';
            paginationDiv.appendChild(ellipsis);
        }
        
        const lastBtn = document.createElement('button');
        lastBtn.innerText = totalPages;
        lastBtn.onclick = () => loadPage(totalPages - 1);
        paginationDiv.appendChild(lastBtn);
    }

    const nextBtn = document.createElement('button');
    nextBtn.innerHTML = 'Next →';
    nextBtn.disabled = currentPage === totalPages - 1;
    nextBtn.onclick = () => loadPage(currentPage + 1);
    paginationDiv.appendChild(nextBtn);
}

function loadTable(sheet, tabElement) {
    console.log('Loading table:', sheet);
    
    if(currentTab) currentTab.classList.remove('active');
    tabElement.classList.add('active');
    currentTab = tabElement;

    currentSheet = sheet;
    currentPage = 0;
    isDataLoaded = false;

    tabElement.classList.add('loading');
    updateStatusBadge('loading', 'Fetching sheet info...');
    showLoadingOverlay();

    fetch('/get_sheet_info?sheet=' + sheet)
        .then(res => res.json())
        .then(data => {
            console.log('Sheet info received:', data);
            totalRows = data.totalRows;
            totalPages = Math.ceil((data.totalRows - 1) / data.pageSize);
            updateRowCount(0, totalRows - 1);
            updatePageInfo(0, totalPages);
            renderPageButtons();
            loadPage(0);
        })
        .catch(error => {
            console.error('Error fetching sheet info:', error);
            updateStatusBadge('error', 'Failed to load sheet');
            hideLoadingOverlay();
            tabElement.classList.remove('loading');
        });
}

function loadPage(page) {
    if (page < 0 || page >= totalPages) return;
    
    console.log('Loading page:', page);
    currentPage = page;
    const progressBar = document.getElementById('progress-bar');
    if (progressBar) {
        progressBar.style.width = '0%';
        progressBar.innerText = '0%';
    }
    
    updateStatusBadge('loading', 'Loading data...');
    showLoadingOverlay();
    isDataLoaded = false;

    const startTime = Date.now();

    fetch(`/get_page?sheet=${currentSheet}&page=${page}`)
        .then(res => res.json())
        .then(data => {
            console.log('Page data received');
            const tableDiv = document.getElementById('table-container');
            if (!tableDiv) {
                console.error('Table container not found');
                return;
            }
            
            const header = data.header;
            const rows = data.rows;
            
            if (!header || rows.length === 0) {
                tableDiv.innerHTML = `
                    <div class="empty-state">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <circle cx="12" cy="12" r="10"></circle>
                            <line x1="12" y1="8" x2="12" y2="12"></line>
                            <line x1="12" y1="16" x2="12.01" y2="16"></line>
                        </svg>
                        <h3>No data available</h3>
                        <p>This page doesn't contain any data</p>
                    </div>
                `;
                if (progressBar) {
                    progressBar.style.width = '100%';
                    progressBar.innerText = '100%';
                }
                updateStatusBadge('error', 'No data');
                hideLoadingOverlay();
                return;
            }

            let html = '<table><thead><tr>';
            header.forEach(h => html += `<th>${h || '-'}</th>`);
            html += '</tr></thead><tbody>';

            const totalRowsToRender = rows.length;
            rows.forEach((row, idx) => {
                html += '<tr>';
                header.forEach((_, colIdx) => {
                    html += `<td>${row[colIdx] || '-'}</td>`;
                });
                html += '</tr>';
                
                if (progressBar) {
                    const percent = Math.floor(((idx + 1) / totalRowsToRender) * 100);
                    progressBar.style.width = percent + '%';
                    progressBar.innerText = percent + '%';
                }
            });
            
            html += '</tbody></table>';
            tableDiv.innerHTML = html;
            
            if (progressBar) {
                progressBar.style.width = '100%';
                progressBar.innerText = '100% Complete';
            }
            
            const loadTime = ((Date.now() - startTime) / 1000).toFixed(2);
            updateStatusBadge('loaded', `✓ Loaded (${loadTime}s)`);
            updateRowCount(rows.length, totalRows - 1);
            updatePageInfo(currentPage, totalPages);
            
            renderPageButtons();
            hideLoadingOverlay();
            
            if (currentTab) currentTab.classList.remove('loading');
            
            isDataLoaded = true;
        })
        .catch(error => {
            console.error('Error loading page:', error);
            updateStatusBadge('error', 'Failed to load page');
            hideLoadingOverlay();
            if (currentTab) currentTab.classList.remove('loading');
        });
}

// ==================== CLEAN DATA FUNCTIONS ====================

function switchDataView(sheetKey, viewType, clickedTab) {
    console.log('Switching data view:', sheetKey, viewType);
    const container = document.getElementById(`${sheetKey}-container`);
    if (!container) {
        console.error('Container not found:', sheetKey);
        return;
    }
    
    container.querySelectorAll('.data-tab').forEach(tab => tab.classList.remove('active'));
    clickedTab.classList.add('active');
    
    container.querySelectorAll('.data-view').forEach(view => view.classList.remove('active'));
    const targetView = document.getElementById(`${sheetKey}-${viewType}`);
    if (targetView) {
        targetView.classList.add('active');
    }
}

function loadOriginalDataForCleaning(sheetKey) {
    console.log('Loading original data for:', sheetKey);
    const loadingOverlay = document.getElementById(`${sheetKey}-original-loading`);
    const tableContainer = document.getElementById(`${sheetKey}-original-table`);
    
    if (!tableContainer) {
        console.error('Table container not found for:', sheetKey);
        return;
    }
    
    if (loadingOverlay) loadingOverlay.classList.add('active');
    
    fetch(`/get_page?sheet=${sheetKey}&page=0`)
        .then(res => res.json())
        .then(data => {
            console.log('Original data loaded for:', sheetKey);
            const header = data.header;
            const rows = data.rows;
            
            let html = '<table><thead><tr>';
            header.forEach(h => html += `<th>${h || '-'}</th>`);
            html += '</tr></thead><tbody>';
            
            rows.slice(0, 100).forEach(row => {
                html += '<tr>';
                header.forEach((_, colIdx) => {
                    html += `<td>${row[colIdx] || '-'}</td>`;
                });
                html += '</tr>';
            });
            
            html += '</tbody></table>';
            if (rows.length > 100) {
                html += '<p style="padding: 10px; text-align: center; color: #6c757d;">Showing first 100 rows...</p>';
            }
            
            tableContainer.innerHTML = html;
            if (loadingOverlay) loadingOverlay.classList.remove('active');
        })
        .catch(error => {
            console.error('Error loading original data:', error);
            tableContainer.innerHTML = '<p style="padding: 20px; text-align: center; color: #dc3545;">Error loading data</p>';
            if (loadingOverlay) loadingOverlay.classList.remove('active');
        });
}

function cleanSheetData(sheetKey, cleanBtn) {
    console.log('Cleaning data for:', sheetKey);
    cleanBtn.disabled = true;
    cleanBtn.textContent = '🔄 Cleaning...';
    
    fetch('/api/clean_data', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({ sheet: sheetKey })
    })
    .then(res => res.json())
    .then(data => {
        console.log('Clean data response:', data);
        if (data.success) {
            const includedTab = document.getElementById(`${sheetKey}-included-tab`);
            const excludedTab = document.getElementById(`${sheetKey}-excluded-tab`);
    
            includedTab.classList.remove('disabled');
            excludedTab.classList.remove('disabled');
    
            includedTab.innerHTML = `Included Rows <span style="background: rgba(255,255,255,0.3); padding: 2px 8px; border-radius: 10px; margin-left: 5px;">${data.summary.included_count}</span>`;
            excludedTab.innerHTML = `Excluded Rows <span style="background: rgba(255,255,255,0.3); padding: 2px 8px; border-radius: 10px; margin-left: 5px;">${data.summary.excluded_count}</span>`;
    
            // Reset pagination to page 1
            cleanedDataPagination[sheetKey].included = 1;
            cleanedDataPagination[sheetKey].excluded = 1;
            
            // Load cleaned data with pagination
            loadCleanedDataWithPagination(sheetKey, 'included', 1);
            loadCleanedDataWithPagination(sheetKey, 'excluded', 1);
        } else {
            alert('Error: ' + (data.error || 'Unknown error'));
        }
        
        cleanBtn.disabled = false;
        cleanBtn.textContent = '🧹 Clean This Sheet';
    })
    .catch(error => {
        console.error('Error cleaning data:', error);
        alert('Error cleaning data: ' + error.message);
        cleanBtn.disabled = false;
        cleanBtn.textContent = '🧹 Clean This Sheet';
    });
}

function loadCleanedDataWithPagination(sheetKey, dataType, page) {
    console.log('Loading cleaned data:', sheetKey, dataType, page);
    const perPage = 100;
    const containerId = `${sheetKey}-${dataType}-table`;
    const container = document.getElementById(containerId);
    
    if (!container) {
        console.error('Container not found:', containerId);
        return;
    }
    
    container.innerHTML = '<p style="padding: 20px; text-align: center; color: #6c757d;">Loading...</p>';
    
    fetch(`/api/get_cleaned_data/${sheetKey}?page=${page}&per_page=${perPage}&type=${dataType}`)
        .then(res => res.json())
        .then(data => {
            console.log('Cleaned data received');
            if (dataType === 'included') {
                renderCleanedTableWithPagination(
                    containerId,
                    data.included_data,
                    ['row_id', 'name', 'birth_day', 'birth_month', 'birth_year'],
                    sheetKey,
                    'included',
                    page,
                    data.included_total_pages,
                    data.total_included
                );
            } else {
                renderCleanedTableWithPagination(
                    containerId,
                    data.excluded_data,
                    ['row_id', 'original_name', 'original_birth_day', 'original_birth_month', 'original_birth_year', 'exclusion_reason'],
                    sheetKey,
                    'excluded',
                    page,
                    data.excluded_total_pages,
                    data.total_excluded
                );
            }
            
            // Show download buttons after data is loaded
            showDownloadButtons(sheetKey, dataType);
        })
        .catch(error => {
            console.error('Error loading cleaned data:', error);
            container.innerHTML = '<p style="padding: 20px; text-align: center; color: #dc3545;">Error loading data</p>';
        });
}

function renderCleanedTableWithPagination(containerId, data, columns, sheetKey, tableType, currentPage, totalPages, totalRecords) {
    const container = document.getElementById(containerId);
    
    if (!container) {
        console.error('Container not found:', containerId);
        return;
    }
    
    if (!data || data.length === 0) {
        container.innerHTML = '<p style="padding: 20px; text-align: center; color: #6c757d;">No data available</p>';
        return;
    }
    
    let html = '<table><thead><tr>';
    columns.forEach(col => html += `<th>${col.replace(/_/g, ' ').toUpperCase()}</th>`);
    html += '</tr></thead><tbody>';
    
    data.forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            html += `<td>${row[col] || '-'}</td>`;
        });
        html += '</tr>';
    });
    
    html += '</tbody></table>';
    
    // Add pagination controls
    if (totalPages > 1) {
        html += '<div style="padding: 15px; text-align: center; border-top: 1px solid #dee2e6; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">';
        
        html += `<span style="color: #6c757d;">Page ${currentPage} of ${totalPages} (${totalRecords.toLocaleString()} total rows)</span>`;
        
        html += '<div style="display: flex; gap: 5px;">';
        
        // First page button
        if (currentPage > 1) {
            html += `<button onclick="loadCleanedDataWithPagination('${sheetKey}', '${tableType}', 1)" style="padding: 8px 12px; cursor: pointer; border: 2px solid #e9ecef; border-radius: 8px; background: white;">« First</button>`;
        }
        
        // Previous button
        if (currentPage > 1) {
            html += `<button onclick="loadCleanedDataWithPagination('${sheetKey}', '${tableType}', ${currentPage - 1})" style="padding: 8px 15px; cursor: pointer; border: 2px solid #e9ecef; border-radius: 8px; background: white;">‹ Previous</button>`;
        } else {
            html += `<button disabled style="padding: 8px 15px; opacity: 0.5; cursor: not-allowed; border: 2px solid #e9ecef; border-radius: 8px; background: white;">‹ Previous</button>`;
        }
        
        // Page numbers
        const maxButtons = 5;
        let startPage = Math.max(1, currentPage - Math.floor(maxButtons / 2));
        let endPage = Math.min(totalPages, startPage + maxButtons - 1);
        
        if (endPage - startPage < maxButtons - 1) {
            startPage = Math.max(1, endPage - maxButtons + 1);
        }
        
        if (startPage > 1) {
            html += '<span style="padding: 8px;">...</span>';
        }
        
        for (let i = startPage; i <= endPage; i++) {
            if (i === currentPage) {
                html += `<button style="padding: 8px 12px; cursor: pointer; border: 2px solid #667eea; border-radius: 8px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; font-weight: 600;">${i}</button>`;
            } else {
                html += `<button onclick="loadCleanedDataWithPagination('${sheetKey}', '${tableType}', ${i})" style="padding: 8px 12px; cursor: pointer; border: 2px solid #e9ecef; border-radius: 8px; background: white;">${i}</button>`;
            }
        }
        
        if (endPage < totalPages) {
            html += '<span style="padding: 8px;">...</span>';
        }
        
        // Next button
        if (currentPage < totalPages) {
            html += `<button onclick="loadCleanedDataWithPagination('${sheetKey}', '${tableType}', ${currentPage + 1})" style="padding: 8px 15px; cursor: pointer; border: 2px solid #e9ecef; border-radius: 8px; background: white;">Next ›</button>`;
        } else {
            html += `<button disabled style="padding: 8px 15px; opacity: 0.5; cursor: not-allowed; border: 2px solid #e9ecef; border-radius: 8px; background: white;">Next ›</button>`;
        }
        
        // Last page button
        if (currentPage < totalPages) {
            html += `<button onclick="loadCleanedDataWithPagination('${sheetKey}', '${tableType}', ${totalPages})" style="padding: 8px 12px; cursor: pointer; border: 2px solid #e9ecef; border-radius: 8px; background: white;">Last »</button>`;
        }
        
        html += '</div></div>';
    }
    
    container.innerHTML = html;
}