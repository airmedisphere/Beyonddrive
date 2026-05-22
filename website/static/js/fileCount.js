// fileCount.js — Stats cards that double as filters
'use strict';

let fileCountData = { videos:0, pdfs:0, images:0, documents:0, audio:0, folders:0, others:0, total:0 };
let activeStatFilter = '';  // '' = all

const fileTypes = {
    video:    ['.mp4','.mkv','.webm','.mov','.avi','.ts','.ogv','.m4v','.flv','.wmv','.3gp','.mpg','.mpeg'],
    pdf:      ['.pdf'],
    image:    ['.jpg','.jpeg','.png','.gif','.bmp','.svg','.webp','.ico','.tiff','.tif'],
    document: ['.doc','.docx','.txt','.rtf','.odt','.pages','.tex','.wpd'],
    audio:    ['.mp3','.wav','.flac','.aac','.ogg','.wma','.m4a','.opus','.aiff']
};

function getFileTypeCategory(fileName) {
    const ext = fileName.toLowerCase().substring(fileName.lastIndexOf('.'));
    for (const [cat, exts] of Object.entries(fileTypes)) {
        if (exts.includes(ext)) return cat;
    }
    return 'others';
}

function countFilesByType(directoryData) {
    const counts = { videos:0, pdfs:0, images:0, documents:0, audio:0, folders:0, others:0, total:0 };
    for (const [, item] of Object.entries(directoryData.contents || {})) {
        if (item.type === 'folder') counts.folders++;
        else if (item.type === 'file') counts[getFileTypeCategory(item.name) + 's']++;
        counts.total++;
    }
    return counts;
}

function applyStatFilter() {
    document.querySelectorAll('.directory-item.file-item').forEach(item => {
        if (!activeStatFilter) { item.style.display = ''; return; }
        const name = item.dataset.name || item.querySelector('.item-title')?.textContent || '';
        const cat  = getFileTypeCategory(name);
        const match =
            (activeStatFilter === 'video'    && cat === 'video')    ||
            (activeStatFilter === 'pdf'      && cat === 'pdf')      ||
            (activeStatFilter === 'image'    && cat === 'image')    ||
            (activeStatFilter === 'audio'    && cat === 'audio')    ||
            (activeStatFilter === 'document' && cat === 'document') ||
            (activeStatFilter === 'folder'   && false);  // folders handled below
        item.style.display = match ? '' : 'none';
    });
    document.querySelectorAll('.directory-item.folder-item').forEach(item => {
        item.style.display = (!activeStatFilter || activeStatFilter === 'folder') ? '' : 'none';
    });
}

function updateFileCountDisplay(counts) {
    fileCountData = counts;
    const container = document.getElementById('file-count-stats');
    if (!container) return;
    if (counts.total === 0) { container.style.display = 'none'; return; }

    container.style.display = 'flex';
    container.innerHTML = '';

    const items = [
        { key: 'total',    filter: '',         label: 'All',     num: counts.total,
          color: '#6366f1', bg: '#eef2ff',
          svg: `<path d="M3 3h7v7H3zm11 0h7v7h-7zM3 14h7v7H3zm11 0h7v7h-7z"/>` },
        { key: 'videos',   filter: 'video',    label: 'Videos',  num: counts.videos,
          color: '#e11d48', bg: '#fff1f2',
          svg: `<polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2"/>` },
        { key: 'pdfs',     filter: 'pdf',      label: 'PDFs',    num: counts.pdfs,
          color: '#d97706', bg: '#fffbeb',
          svg: `<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14,2 14,8 20,8"/>` },
        { key: 'images',   filter: 'image',    label: 'Images',  num: counts.images,
          color: '#059669', bg: '#ecfdf5',
          svg: `<rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21,15 16,10 5,21"/>` },
        { key: 'folders',  filter: 'folder',   label: 'Modules', num: counts.folders,
          color: '#0284c7', bg: '#f0f9ff',
          svg: `<path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>` },
        { key: 'audio',    filter: 'audio',    label: 'Audio',   num: counts.audio,
          color: '#7c3aed', bg: '#f5f3ff',
          svg: `<path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/>` },
    ];

    items.forEach(({ key, filter, label, num, color, bg, svg }) => {
        if (num === 0 && key !== 'total') return;
        const el = document.createElement('button');
        el.className = 'stat-filter-card' + (activeStatFilter === filter ? ' active' : '');
        el.dataset.filter = filter;
        el.style.setProperty('--sc', color);
        el.style.setProperty('--sb', bg);
        el.innerHTML = `
            <div class="sfc-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">${svg}</svg></div>
            <div class="sfc-label">${label}</div>
            <div class="sfc-num">${num}</div>
        `;
        el.addEventListener('click', () => {
            activeStatFilter = (activeStatFilter === filter) ? '' : filter;
            container.querySelectorAll('.stat-filter-card').forEach(c => {
                c.classList.toggle('active', c.dataset.filter === activeStatFilter);
            });
            applyStatFilter();
        });
        container.appendChild(el);
    });

    // Re-apply active filter after re-render
    if (activeStatFilter) setTimeout(applyStatFilter, 50);

    // Watch for DOM changes to re-apply filter
    const dirData = document.getElementById('directory-data');
    if (dirData && !dirData._statFilterObserver) {
        dirData._statFilterObserver = true;
        new MutationObserver(() => { if (activeStatFilter) setTimeout(applyStatFilter, 80); })
            .observe(dirData, { childList: true, subtree: true });
    }
}

function shouldShowFileCount(path) {
    return !path.startsWith('/trash') && !path.startsWith('/search_') && !path.startsWith('/share_');
}

function updateFileCount(directoryData) {
    const path = getCurrentPath();
    const container = document.getElementById('file-count-stats');
    if (shouldShowFileCount(path)) {
        updateFileCountDisplay(countFilesByType(directoryData));
    } else if (container) {
        container.style.display = 'none';
    }
}

window.updateFileCount = updateFileCount;
