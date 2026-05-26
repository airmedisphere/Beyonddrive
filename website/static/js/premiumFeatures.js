// Premium Features JavaScript
document.addEventListener('DOMContentLoaded', function() {
    // Feature toggle functionality
    const featureToggle = document.getElementById('feature-toggle');
    const featuresPanel = document.getElementById('features-panel');
    const bulkSelectToggle = document.getElementById('bulk-select-toggle');
    const gridViewToggle = document.getElementById('grid-view-toggle');
    const smartSearchToggle = document.getElementById('smart-search-toggle');
    
    // Quick actions
    const quickUpload = document.getElementById('quick-upload');
    const quickFolder = document.getElementById('quick-folder');
    const quickSearch = document.getElementById('quick-search');
    
    // View toggles
    const listViewBtn = document.getElementById('list-view-btn');
    const gridViewBtn = document.getElementById('grid-view-btn');
    const listView = document.getElementById('list-view');
    const gridView = document.getElementById('grid-view');
    
    // Bulk actions
    const bulkActions = document.getElementById('bulk-actions');
    const bulkCount = document.getElementById('bulk-count');
    const bulkMove = document.getElementById('bulk-move');
    const bulkCopy = document.getElementById('bulk-copy');
    const bulkDelete = document.getElementById('bulk-delete');
    const bulkSelectAll = document.getElementById('bulk-select-all');
    const bulkDeselectAll = document.getElementById('bulk-deselect-all');
    const bulkDeletePermanent = document.getElementById('bulk-delete-permanent');
    
    // Search suggestions
    const fileSearch = document.getElementById('file-search');
    const searchSuggestions = document.getElementById('search-suggestions');
    
    let selectedItems = new Set();
    let isBulkSelectEnabled = false;
    let isGridViewEnabled = false;
    let isSmartSearchEnabled = true;
    
    // Load saved preferences
    loadPreferences();
    
    // Feature panel toggle
    featureToggle.addEventListener('click', function() {
        featuresPanel.classList.toggle('show');
    });
    
    // Close features panel when clicking outside
    document.addEventListener('click', function(e) {
        if (!featureToggle.contains(e.target) && !featuresPanel.contains(e.target)) {
            featuresPanel.classList.remove('show');
        }
    });
    
    // Feature toggles
    bulkSelectToggle.addEventListener('click', function() {
        isBulkSelectEnabled = !isBulkSelectEnabled;
        this.classList.toggle('active');
        toggleBulkSelect();
        savePreferences();
    });
    
    gridViewToggle.addEventListener('click', function() {
        isGridViewEnabled = !isGridViewEnabled;
        this.classList.toggle('active');
        toggleGridView();
        savePreferences();
    });
    
    smartSearchToggle.addEventListener('click', function() {
        isSmartSearchEnabled = !isSmartSearchEnabled;
        this.classList.toggle('active');
        toggleSmartSearch();
        savePreferences();
    });
    
    // Quick actions
    quickUpload.addEventListener('click', function() {
        document.getElementById('fileInput').click();
    });
    
    quickFolder.addEventListener('click', function() {
        document.getElementById('new-folder-btn').click();
    });
    
    quickSearch.addEventListener('click', function() {
        document.getElementById('file-search').focus();
    });
    
    // View toggles
    listViewBtn.addEventListener('click', function() {
        if (!this.classList.contains('active')) {
            switchToListView();
        }
    });
    
    gridViewBtn.addEventListener('click', function() {
        if (!this.classList.contains('active')) {
            switchToGridView();
        }
    });
    
    // Bulk actions
    bulkMove.addEventListener('click', function() {
        if (selectedItems.size > 0) {
            // Implement bulk move functionality
            console.log('Bulk move:', Array.from(selectedItems));
        }
    });
    
    bulkCopy.addEventListener('click', function() {
        if (selectedItems.size > 0) {
            // Implement bulk copy functionality
            console.log('Bulk copy:', Array.from(selectedItems));
        }
    });
    
    bulkDelete.addEventListener('click', async function() {
        if (selectedItems.size === 0) return;
        if (!confirm(`Send ${selectedItems.size} item(s) to Archive?`)) return;
        await bulkArchiveOrDelete(true);
    });

    if (bulkDeletePermanent) {
        bulkDeletePermanent.addEventListener('click', async function() {
            if (selectedItems.size === 0) return;
            if (!confirm(`⚠️ PERMANENTLY DELETE ${selectedItems.size} item(s)?\n\nThis CANNOT be undone.`)) return;
            await bulkArchiveOrDelete(false);
        });
    }

    if (bulkSelectAll) {
        bulkSelectAll.addEventListener('click', function() {
            const checkboxes = document.querySelectorAll('.directory-item .selection-checkbox');
            checkboxes.forEach(cb => {
                if (!cb.checked) {
                    cb.checked = true;
                    const item = cb.closest('.directory-item');
                    const id = item.getAttribute('data-id');
                    if (id) selectedItems.add(id);
                }
            });
            updateBulkActions();
            bulkSelectAll.style.display = 'none';
            if (bulkDeselectAll) bulkDeselectAll.style.display = 'inline-block';
        });
    }

    if (bulkDeselectAll) {
        bulkDeselectAll.addEventListener('click', function() {
            const checkboxes = document.querySelectorAll('.directory-item .selection-checkbox');
            checkboxes.forEach(cb => { cb.checked = false; });
            selectedItems.clear();
            updateBulkActions();
            bulkDeselectAll.style.display = 'none';
            if (bulkSelectAll) bulkSelectAll.style.display = 'inline-block';
        });
    }

    // Show "Delete Permanently" button only when viewing /trash
    if (window.location.pathname === '/' && window.location.search.includes('path=/trash')) {
        if (bulkDeletePermanent) bulkDeletePermanent.style.display = 'inline-block';
        if (bulkDelete) bulkDelete.style.display = 'none';
    }

    async function bulkArchiveOrDelete(archiveOnly) {
        const items = Array.from(selectedItems);
        const total = items.length;

        // Collect paths from the DOM (selectedItems only has ids)
        const tasks = [];
        for (const id of items) {
            const itemEl = document.querySelector(`.directory-item[data-id="${id}"]`);
            if (!itemEl) continue;
            const itemPath = itemEl.getAttribute('data-path');
            if (!itemPath) continue;
            tasks.push({ id, path: itemPath + '/' + id });
        }

        const total_real = tasks.length;
        if (total_real === 0) {
            alert('Could not resolve item paths. Reload and try again.');
            return;
        }

        // Show a simple inline progress indicator on the toolbar
        const originalText = bulkCount.textContent;
        let done = 0, errors = 0;

        // Concurrency limit — 4 parallel requests is enough; more risks rate limits
        const CONCURRENCY = 4;
        let cursor = 0;
        async function worker() {
            while (cursor < tasks.length) {
                const my = cursor++;
                const t = tasks[my];
                try {
                    let resp;
                    if (archiveOnly) {
                        resp = await postJson('/api/trashFileFolder', { path: t.path, trash: true });
                    } else {
                        resp = await postJson('/api/deleteFileFolder', { path: t.path });
                    }
                    if (resp && resp.status === 'ok') {
                        done++;
                    } else {
                        errors++;
                    }
                } catch (e) {
                    errors++;
                }
                bulkCount.textContent = `${done + errors}/${total_real} processed (${errors} errors)`;
            }
        }
        const workers = [];
        for (let i = 0; i < CONCURRENCY; i++) workers.push(worker());
        await Promise.all(workers);

        alert(
            archiveOnly
                ? `✅ Archived ${done} item(s).\nErrors: ${errors}`
                : `🗑️ Permanently deleted ${done} item(s).\nErrors: ${errors}`
        );
        window.location.reload();
    }
    
    // Smart search functionality
    if (isSmartSearchEnabled) {
        fileSearch.addEventListener('input', handleSmartSearch);
        fileSearch.addEventListener('focus', showSearchSuggestions);
        fileSearch.addEventListener('blur', hideSearchSuggestions);
    }
    
    function toggleBulkSelect() {
        const directoryItems = document.querySelectorAll('.directory-item');
        
        if (isBulkSelectEnabled) {
            // Add checkboxes to all items
            directoryItems.forEach(item => {
                if (!item.querySelector('.selection-checkbox')) {
                    const checkbox = document.createElement('input');
                    checkbox.type = 'checkbox';
                    checkbox.className = 'selection-checkbox';
                    checkbox.addEventListener('change', handleItemSelection);
                    
                    const itemName = item.querySelector('.item-name');
                    itemName.insertBefore(checkbox, itemName.firstChild);
                }
            });
        } else {
            // Remove checkboxes
            directoryItems.forEach(item => {
                const checkbox = item.querySelector('.selection-checkbox');
                if (checkbox) {
                    checkbox.remove();
                }
            });
            selectedItems.clear();
            updateBulkActions();
        }
    }
    
    function handleItemSelection(e) {
        e.stopPropagation();
        const item = e.target.closest('.directory-item');
        const itemId = item.getAttribute('data-id');
        
        if (e.target.checked) {
            selectedItems.add(itemId);
        } else {
            selectedItems.delete(itemId);
        }
        
        updateBulkActions();
    }
    
    function updateBulkActions() {
        const count = selectedItems.size;
        bulkCount.textContent = `${count} item${count !== 1 ? 's' : ''} selected`;
        
        if (count > 0) {
            bulkActions.classList.add('show');
        } else {
            bulkActions.classList.remove('show');
        }
    }
    
    function toggleGridView() {
        if (isGridViewEnabled) {
            switchToGridView();
        } else {
            switchToListView();
        }
    }
    
    function switchToListView() {
        listViewBtn.classList.add('active');
        gridViewBtn.classList.remove('active');
        listView.style.display = 'flex';
        gridView.style.display = 'none';
        isGridViewEnabled = false;
        savePreferences();
    }
    
    function switchToGridView() {
        gridViewBtn.classList.add('active');
        listViewBtn.classList.remove('active');
        listView.style.display = 'none';
        gridView.style.display = 'grid';
        isGridViewEnabled = true;
        populateGridView();
        savePreferences();
    }
    
    function populateGridView() {
        const listItems = document.querySelectorAll('#directory-data .directory-item');
        gridView.innerHTML = '';
        
        listItems.forEach(item => {
            const itemName = item.querySelector('.item-title').textContent;
            const itemSize = item.querySelector('.item-size').textContent;
            const isFolder = item.classList.contains('folder-item');
            const itemId = item.getAttribute('data-id');
            const itemPath = item.getAttribute('data-path');
            
            const gridItem = document.createElement('div');
            gridItem.className = 'grid-item';
            gridItem.setAttribute('data-id', itemId);
            gridItem.setAttribute('data-path', itemPath);
            
            if (isFolder) {
                gridItem.classList.add('folder-item');
            } else {
                gridItem.classList.add('file-item');
            }
            
            gridItem.innerHTML = `
                <div class="grid-icon ${isFolder ? 'folder' : 'file'}">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        ${isFolder ? 
                            '<path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>' :
                            '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14,2 14,8 20,8"/>'
                        }
                    </svg>
                </div>
                <div class="grid-title">${itemName}</div>
                <div class="grid-meta">${isFolder ? 'Folder' : itemSize}</div>
            `;
            
            // Add click handlers
            if (isFolder) {
                gridItem.addEventListener('dblclick', openFolder);
            } else {
                gridItem.addEventListener('dblclick', openFile);
            }
            
            gridView.appendChild(gridItem);
        });
    }
    
    function toggleSmartSearch() {
        if (isSmartSearchEnabled) {
            fileSearch.addEventListener('input', handleSmartSearch);
            fileSearch.addEventListener('focus', showSearchSuggestions);
            fileSearch.addEventListener('blur', hideSearchSuggestions);
        } else {
            fileSearch.removeEventListener('input', handleSmartSearch);
            fileSearch.removeEventListener('focus', showSearchSuggestions);
            fileSearch.removeEventListener('blur', hideSearchSuggestions);
            hideSearchSuggestions();
        }
    }
    
    function handleSmartSearch(e) {
        const query = e.target.value.toLowerCase();
        
        if (query.length < 2) {
            hideSearchSuggestions();
            return;
        }
        
        // Generate smart suggestions based on current directory
        const suggestions = generateSearchSuggestions(query);
        displaySearchSuggestions(suggestions);
    }
    
    function generateSearchSuggestions(query) {
        const suggestions = [];
        const directoryItems = document.querySelectorAll('.directory-item');
        
        // File type suggestions
        const fileTypes = ['pdf', 'video', 'image', 'document', 'audio'];
        fileTypes.forEach(type => {
            if (type.includes(query)) {
                suggestions.push({
                    text: `All ${type} files`,
                    type: 'filter',
                    icon: getFileTypeIcon(type),
                    action: () => filterByType(type)
                });
            }
        });
        
        // Recent suggestions
        if (query === 'recent' || 'recent'.includes(query)) {
            suggestions.push({
                text: 'Recently added files',
                type: 'filter',
                icon: '<path d="M12 2v10l4 4"/>',
                action: () => sortByDate()
            });
        }
        
        // Size suggestions
        if (query === 'large' || 'large'.includes(query)) {
            suggestions.push({
                text: 'Large files (>100MB)',
                type: 'filter',
                icon: '<path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/>',
                action: () => filterBySize('large')
            });
        }
        
        // File name suggestions
        directoryItems.forEach(item => {
            const fileName = item.querySelector('.item-title').textContent.toLowerCase();
            if (fileName.includes(query) && suggestions.length < 8) {
                suggestions.push({
                    text: item.querySelector('.item-title').textContent,
                    type: 'file',
                    icon: item.classList.contains('folder-item') ? 
                        '<path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>' :
                        '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>',
                    action: () => highlightItem(item)
                });
            }
        });
        
        return suggestions.slice(0, 6);
    }
    
    function displaySearchSuggestions(suggestions) {
        searchSuggestions.innerHTML = '';
        
        if (suggestions.length === 0) {
            hideSearchSuggestions();
            return;
        }
        
        suggestions.forEach(suggestion => {
            const suggestionElement = document.createElement('div');
            suggestionElement.className = 'search-suggestion';
            suggestionElement.innerHTML = `
                <svg class="suggestion-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    ${suggestion.icon}
                </svg>
                <span class="suggestion-text">${suggestion.text}</span>
                <span class="suggestion-type">${suggestion.type}</span>
            `;
            
            suggestionElement.addEventListener('click', function() {
                suggestion.action();
                hideSearchSuggestions();
            });
            
            searchSuggestions.appendChild(suggestionElement);
        });
        
        showSearchSuggestions();
    }
    
    function showSearchSuggestions() {
        searchSuggestions.style.display = 'block';
    }
    
    function hideSearchSuggestions() {
        setTimeout(() => {
            searchSuggestions.style.display = 'none';
        }, 200);
    }
    
    function getFileTypeIcon(type) {
        const icons = {
            pdf: '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>',
            video: '<polygon points="23 7 16 12 23 17 23 7"/>',
            image: '<rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>',
            document: '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>',
            audio: '<path d="M9 18V5l12-2v13"/>'
        };
        return icons[type] || icons.document;
    }
    
    function filterByType(type) {
        // Implement file type filtering
        console.log('Filter by type:', type);
    }
    
    function sortByDate() {
        // Implement date sorting
        document.getElementById('sort-by').value = 'date';
        document.getElementById('sort-by').dispatchEvent(new Event('change'));
    }
    
    function filterBySize(size) {
        // Implement size filtering
        console.log('Filter by size:', size);
    }
    
    function highlightItem(item) {
        // Scroll to and highlight the item
        item.scrollIntoView({ behavior: 'smooth', block: 'center' });
        item.style.background = 'var(--warning-100)';
        setTimeout(() => {
            item.style.background = '';
        }, 2000);
    }
    
    function savePreferences() {
        const preferences = {
            bulkSelect: isBulkSelectEnabled,
            gridView: isGridViewEnabled,
            smartSearch: isSmartSearchEnabled
        };
        localStorage.setItem('premiumFeatures', JSON.stringify(preferences));
    }
    
    function loadPreferences() {
        const saved = localStorage.getItem('premiumFeatures');
        if (saved) {
            const preferences = JSON.parse(saved);
            
            if (preferences.bulkSelect) {
                isBulkSelectEnabled = true;
                bulkSelectToggle.classList.add('active');
            }
            
            if (preferences.gridView) {
                isGridViewEnabled = true;
                gridViewToggle.classList.add('active');
            }
            
            if (preferences.smartSearch !== undefined) {
                isSmartSearchEnabled = preferences.smartSearch;
                if (isSmartSearchEnabled) {
                    smartSearchToggle.classList.add('active');
                } else {
                    smartSearchToggle.classList.remove('active');
                }
            }
        }
    }
    
    // Initialize features based on saved preferences
    function initializeFeatures() {
        if (isBulkSelectEnabled) {
            toggleBulkSelect();
        }
        
        if (isGridViewEnabled) {
            switchToGridView();
        }
        
        if (isSmartSearchEnabled) {
            toggleSmartSearch();
        }
    }
    
    // Initialize after directory is loaded
    setTimeout(initializeFeatures, 1000);
    
    // Export functions for use in other scripts
    window.premiumFeatures = {
        toggleBulkSelect,
        populateGridView,
        updateBulkActions,
        selectedItems
    };
});