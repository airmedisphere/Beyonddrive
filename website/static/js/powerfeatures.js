// ============================================================
//  POWER FEATURES — loaded after all other scripts
// ============================================================

// ── 1. RECENT FILES ─────────────────────────────────────────
const RECENT_KEY = 'bd_recent_files';
const RECENT_MAX = 10;

function getRecentFiles() {
    try { return JSON.parse(localStorage.getItem(RECENT_KEY) || '[]'); }
    catch { return []; }
}

function addRecentFile(name, path, id) {
    const recent = getRecentFiles().filter(f => f.id !== id);
    recent.unshift({ name, path, id, ts: Date.now() });
    if (recent.length > RECENT_MAX) recent.pop();
    localStorage.setItem(RECENT_KEY, JSON.stringify(recent));
}

function renderRecentPanel() {
    const panel = document.getElementById('recent-panel');
    if (!panel) return;
    const recent = getRecentFiles();
    if (!recent.length) {
        panel.innerHTML = '<div class="recent-empty">No recently opened files</div>';
        return;
    }
    panel.innerHTML = recent.map(f => `
        <div class="recent-item" data-path="${f.path}" data-id="${f.id}" data-name="${f.name}">
            <span class="recent-icon">${getRecentIcon(f.name)}</span>
            <span class="recent-name">${f.name}</span>
        </div>
    `).join('');
    panel.querySelectorAll('.recent-item').forEach(el => {
        el.addEventListener('click', function () {
            const name = this.dataset.name.toLowerCase();
            const rawPath = this.dataset.path + '/' + this.dataset.id;
            const imageExts = ['.jpg','.jpeg','.png','.gif','.webp','.bmp','.svg','.ico','.avif'];
            const audioExts = ['.mp3','.wav','.flac','.aac','.ogg','.m4a','.opus','.wma'];
            if (name.endsWith('.pdf') || name.endsWith('.epub'))
                window.open('/pdf-viewer?path=' + encodeURIComponent(rawPath), '_blank');
            else if (imageExts.some(e => name.endsWith(e)))
                window.open('/image-viewer?path=' + encodeURIComponent(rawPath), '_blank');
            else if (audioExts.some(e => name.endsWith(e)))
                window.open('/audio-player?path=' + encodeURIComponent(rawPath), '_blank');
            else
                window.open('/file?path=' + rawPath, '_blank');
        });
    });
}

function getRecentIcon(name) {
    const n = name.toLowerCase();
    const imageExts = ['.jpg','.jpeg','.png','.gif','.webp','.bmp','.svg'];
    const audioExts = ['.mp3','.wav','.flac','.aac','.ogg','.m4a','.opus','.wma'];
    const videoExts = ['.mp4','.mkv','.webm','.mov','.avi','.ts'];
    if (n.endsWith('.pdf')) return '📄';
    if (imageExts.some(e => n.endsWith(e))) return '🖼️';
    if (audioExts.some(e => n.endsWith(e))) return '🎵';
    if (videoExts.some(e => n.endsWith(e))) return '🎬';
    return '📁';
}

// Intercept file open clicks to track recent files
document.addEventListener('click', function(e) {
    const fileItem = e.target.closest('.file-item[data-name]');
    if (fileItem && !e.target.closest('.more-btn') && !e.target.closest('.more-options')) {
        const name = fileItem.dataset.name;
        const path = fileItem.dataset.path;
        const id   = fileItem.dataset.id;
        if (name && path !== undefined && id) addRecentFile(name, path, id);
    }
}, true);


// ── 2. DRAG & DROP + PASTE UPLOAD ───────────────────────────
(function () {
    let dragCounter = 0;
    const overlay = document.createElement('div');
    overlay.id = 'drop-overlay';
    overlay.innerHTML = `
        <div class="drop-inner">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="64" height="64">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                <polyline points="17 8 12 3 7 8"/>
                <line x1="12" y1="3" x2="12" y2="15"/>
            </svg>
            <h2>Drop files to upload</h2>
            <p>Release to upload to current folder</p>
        </div>`;
    document.body.appendChild(overlay);

    document.addEventListener('dragenter', e => {
        if (!e.dataTransfer.types.includes('Files')) return;
        dragCounter++;
        overlay.classList.add('active');
    });
    document.addEventListener('dragleave', () => {
        dragCounter--;
        if (dragCounter <= 0) { dragCounter = 0; overlay.classList.remove('active'); }
    });
    document.addEventListener('dragover', e => e.preventDefault());
    document.addEventListener('drop', e => {
        e.preventDefault();
        dragCounter = 0;
        overlay.classList.remove('active');
        const files = e.dataTransfer.files;
        if (files.length) triggerFileUpload(files[0]);
    });

    // Ctrl+V paste (images from clipboard)
    document.addEventListener('paste', e => {
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
        const items = Array.from(e.clipboardData.items || []);
        const imgItem = items.find(i => i.type.startsWith('image/'));
        if (!imgItem) return;
        const blob = imgItem.getAsFile();
        if (!blob) return;
        const ext = blob.type.split('/')[1] || 'png';
        const now = new Date();
        const ts  = `${now.getFullYear()}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}_${String(now.getHours()).padStart(2,'0')}${String(now.getMinutes()).padStart(2,'0')}${String(now.getSeconds()).padStart(2,'0')}`;
        const file = new File([blob], `paste_${ts}.${ext}`, { type: blob.type });
        showToast('📋 Pasted image — uploading…');
        triggerFileUpload(file);
    });

    function triggerFileUpload(file) {
        const input = document.getElementById('fileInput');
        if (!input) return;
        const dt = new DataTransfer();
        dt.items.add(file);
        input.files = dt.files;
        input.dispatchEvent(new Event('change', { bubbles: true }));
    }
})();


// ── 3. RIGHT-CLICK CONTEXT MENU ─────────────────────────────
(function () {
    const menu = document.createElement('div');
    menu.id = 'ctx-menu';
    document.body.appendChild(menu);

    let ctxTarget = null;

    document.addEventListener('contextmenu', e => {
        const item = e.target.closest('.directory-item');
        if (!item) { closeCtx(); return; }
        e.preventDefault();
        ctxTarget = item;
        const isFolder = item.classList.contains('folder-item');
        const name = item.dataset.name || item.querySelector('.item-title')?.textContent || '';
        const id   = item.dataset.id;
        const path = item.dataset.path;

        const actions = isFolder ? [
            { icon: '📂', label: 'Open', fn: () => item.click() },
            { icon: '✏️', label: 'Rename', fn: () => document.getElementById(`rename-${id}`)?.click() },
            { icon: '📋', label: 'Copy', fn: () => document.getElementById(`copy-${id}`)?.click() },
            { icon: '🔒', label: 'Lock Folder', fn: () => document.getElementById(`lock-folder-${id}`)?.click() },
            { icon: '🔗', label: 'Share Link', fn: () => document.getElementById(`folder-share-${id}`)?.click() },
            { icon: '🗑️', label: 'Archive', fn: () => document.getElementById(`trash-${id}`)?.click() },
        ] : [
            { icon: '▶️', label: 'Open', fn: () => item.click() },
            { icon: '⬇️', label: 'Download', fn: () => window.open('/file?path=' + path + '/' + id, '_blank') },
            { icon: '✏️', label: 'Rename', fn: () => document.getElementById(`rename-${id}`)?.click() },
            { icon: '📋', label: 'Copy', fn: () => document.getElementById(`copy-${id}`)?.click() },
            { icon: 'ℹ️', label: 'File Info', fn: () => showFileInfo(item) },
            { icon: '🗑️', label: 'Archive', fn: () => document.getElementById(`trash-${id}`)?.click() },
        ];

        menu.innerHTML = actions.map(a =>
            `<div class="ctx-item"><span class="ctx-icon">${a.icon}</span>${a.label}</div>`
        ).join('');

        menu.querySelectorAll('.ctx-item').forEach((el, i) => {
            el.addEventListener('click', () => { actions[i].fn(); closeCtx(); });
        });

        // Position
        const { clientX: x, clientY: y } = e;
        const vw = window.innerWidth, vh = window.innerHeight;
        menu.style.display = 'block';
        const mw = menu.offsetWidth, mh = menu.offsetHeight;
        menu.style.left = (x + mw > vw ? x - mw : x) + 'px';
        menu.style.top  = (y + mh > vh ? y - mh : y) + 'px';
        menu.classList.add('open');
    });

    document.addEventListener('click', closeCtx);
    document.addEventListener('keydown', e => { if (e.key === 'Escape') closeCtx(); });

    function closeCtx() {
        menu.classList.remove('open');
        menu.style.display = 'none';
    }
})();


// ── 4. FILE INFO PANEL ──────────────────────────────────────
function showFileInfo(item) {
    const name = item.dataset.name || item.querySelector('.item-title')?.textContent || 'Unknown';
    const size = item.querySelector('.item-size')?.textContent || '-';
    const dur  = item.querySelector('.duration-badge')?.textContent || '-';
    const path = (item.dataset.path || '') + '/' + (item.dataset.id || '');
    const ext  = name.includes('.') ? name.split('.').pop().toUpperCase() : 'File';

    const existing = document.getElementById('file-info-panel');
    if (existing) existing.remove();

    const panel = document.createElement('div');
    panel.id = 'file-info-panel';
    panel.innerHTML = `
        <div class="fip-header">
            <span>File Info</span>
            <button id="fip-close">✕</button>
        </div>
        <div class="fip-icon">${getRecentIcon(name)}</div>
        <div class="fip-name">${name}</div>
        <table class="fip-table">
            <tr><td>Type</td><td>${ext}</td></tr>
            <tr><td>Size</td><td>${size}</td></tr>
            ${dur !== '-' ? `<tr><td>Duration</td><td>${dur}</td></tr>` : ''}
            <tr><td>Path</td><td class="fip-path">${path}</td></tr>
        </table>
        <div class="fip-actions">
            <button class="fip-btn" id="fip-open">Open</button>
            <button class="fip-btn fip-btn-dl" id="fip-dl">Download</button>
        </div>`;
    document.body.appendChild(panel);

    document.getElementById('fip-close').onclick = () => panel.remove();
    document.getElementById('fip-open').onclick = () => { item.click(); panel.remove(); };
    document.getElementById('fip-dl').onclick = () => window.open('/file?path=' + path, '_blank');

    // Animate in
    requestAnimationFrame(() => panel.classList.add('fip-visible'));
}


// ── 5. IMAGE THUMBNAILS IN GRID VIEW ────────────────────────
const _origPopulate = window.__populateGridViewPatched;

function patchGridThumbnails() {
    const gridView = document.getElementById('grid-view');
    if (!gridView) return;

    const observer = new MutationObserver(() => {
        gridView.querySelectorAll('.grid-item:not([data-thumb-checked])').forEach(el => {
            el.setAttribute('data-thumb-checked', '1');
            const name = el.querySelector('.grid-title')?.textContent || '';
            const isFolder = el.classList.contains('folder-item');
            if (isFolder) return;

            const imageExts = ['.jpg','.jpeg','.png','.gif','.webp','.bmp','.avif'];
            if (!imageExts.some(e => name.toLowerCase().endsWith(e))) return;

            // Get path from matching list item
            const id   = el.dataset.id;
            const path = el.dataset.path;
            if (!id || path === undefined) return;

            const imgUrl = '/file?path=' + path + '/' + id;
            const iconEl = el.querySelector('.grid-icon');
            if (!iconEl) return;

            const img = document.createElement('img');
            img.src = imgUrl;
            img.className = 'grid-thumb';
            img.alt = name;
            img.loading = 'lazy';
            img.onload  = () => { iconEl.innerHTML = ''; iconEl.appendChild(img); iconEl.classList.add('thumb'); };
            img.onerror = () => {};
        });
    });

    observer.observe(gridView, { childList: true, subtree: true });
}


// ── 6. KEYBOARD SHORTCUTS PANEL ─────────────────────────────
(function () {
    const shortcuts = [
        { key: '/', desc: 'Focus search' },
        { key: '?', desc: 'Show this help' },
        { key: 'Esc', desc: 'Close panel / Cancel' },
        { key: 'U', desc: 'Upload file' },
        { key: 'N', desc: 'New folder' },
        { key: 'R', desc: 'Refresh directory' },
        { key: 'Del', desc: 'Archive selected (bulk)' },
        { key: 'Ctrl+V', desc: 'Paste image from clipboard' },
        { key: 'Drag & Drop', desc: 'Drop files to upload' },
        { key: 'Right-click', desc: 'Context menu on items' },
    ];

    const modal = document.createElement('div');
    modal.id = 'shortcuts-modal';
    modal.innerHTML = `
        <div class="sc-box">
            <div class="sc-header"><span>⌨️ Keyboard Shortcuts</span><button id="sc-close">✕</button></div>
            <div class="sc-list">
                ${shortcuts.map(s => `
                    <div class="sc-row">
                        <kbd class="sc-key">${s.key}</kbd>
                        <span class="sc-desc">${s.desc}</span>
                    </div>`).join('')}
            </div>
        </div>`;
    document.body.appendChild(modal);

    document.getElementById('sc-close').onclick = () => modal.classList.remove('open');

    document.addEventListener('keydown', e => {
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
        if (e.key === '?') { e.preventDefault(); modal.classList.toggle('open'); return; }
        if (e.key === 'Escape') { modal.classList.remove('open'); return; }
        if (e.key === '/') { e.preventDefault(); document.getElementById('file-search')?.focus(); return; }
        if (e.key === 'u' || e.key === 'U') { if (!modal.classList.contains('open')) document.getElementById('fileInput')?.click(); return; }
        if (e.key === 'n' || e.key === 'N') { if (!modal.classList.contains('open')) document.getElementById('new-folder-btn')?.click(); return; }
        if (e.key === 'r' || e.key === 'R') { if (!modal.classList.contains('open')) getCurrentDirectory?.(); return; }
    });
    modal.addEventListener('click', e => { if (e.target === modal) modal.classList.remove('open'); });
})();


// ── 7. TOAST NOTIFICATION SYSTEM ────────────────────────────
function showToast(msg, duration = 3000, type = 'info') {
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        document.body.appendChild(container);
    }
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = msg;
    container.appendChild(toast);
    requestAnimationFrame(() => toast.classList.add('toast-show'));
    setTimeout(() => {
        toast.classList.remove('toast-show');
        setTimeout(() => toast.remove(), 400);
    }, duration);
}

// Override native alert for common messages
const _nativeAlert = window.alert;
window.alert = function(msg) {
    if (typeof msg === 'string' && msg.length < 120 && !msg.includes('\n\n')) {
        const type = msg.includes('❌') || msg.includes('Error') || msg.includes('failed') ? 'error'
                   : msg.includes('✅') || msg.includes('Completed') || msg.includes('success') ? 'success'
                   : 'info';
        showToast(msg, 4000, type);
    } else {
        _nativeAlert(msg);
    }
};


// ── 8. KEYBOARD SHORTCUT HINT BUTTON IN TOOLBAR ─────────────
document.addEventListener('DOMContentLoaded', () => {
    // Render recent panel if it exists
    renderRecentPanel();

    // Patch grid thumbnails
    patchGridThumbnails();

    // Add ? button to toolbar area
    const header = document.querySelector('.header-actions') || document.querySelector('.top-bar');
    if (header) {
        const btn = document.createElement('button');
        btn.className = 'tb-shortcut-hint';
        btn.title = 'Keyboard shortcuts (?)';
        btn.textContent = '?';
        btn.onclick = () => document.getElementById('shortcuts-modal')?.classList.add('open');
        header.appendChild(btn);
    }

    // Welcome toast disabled
});
