// ============================================================
//  NEW FEATURES — EduMaster Pro Enhancements
// ============================================================

// ── 1. DARK / LIGHT THEME TOGGLE ────────────────────────────
(function () {
    const THEME_KEY = 'bd_theme';

    function applyTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem(THEME_KEY, theme);
        const btn = document.getElementById('theme-toggle-btn');
        if (btn) btn.title = theme === 'dark' ? 'Switch to Light Mode' : 'Switch to Dark Mode';
        if (btn) btn.innerHTML = theme === 'dark' ? '☀️' : '🌙';
    }

    function initTheme() {
        const saved = localStorage.getItem(THEME_KEY) || 'light';
        applyTheme(saved);
    }

    document.addEventListener('DOMContentLoaded', () => {
        initTheme();

        // Inject toggle button into toolbar
        const toolbar = document.querySelector('.toolbar');
        if (toolbar) {
            const btn = document.createElement('button');
            btn.id = 'theme-toggle-btn';
            btn.className = 'sort-order-btn theme-toggle-btn';
            btn.style.cssText = 'font-size:1rem;min-width:30px;height:30px;';
            btn.addEventListener('click', () => {
                const current = document.documentElement.getAttribute('data-theme') || 'light';
                applyTheme(current === 'dark' ? 'light' : 'dark');
            });
            toolbar.appendChild(btn);
        }

        // Also init immediately so theme applies before DOMContentLoaded renders
        initTheme();
    });

    // Apply immediately (before DOMContentLoaded flicker)
    initTheme();
})();


// ── 2. FAVORITES / BOOKMARKS ─────────────────────────────────
(function () {
    const FAV_KEY = 'bd_favorites';

    function getFavs() {
        try { return JSON.parse(localStorage.getItem(FAV_KEY) || '[]'); }
        catch { return []; }
    }

    function saveFavs(favs) {
        localStorage.setItem(FAV_KEY, JSON.stringify(favs));
    }

    function isFav(id) {
        return getFavs().some(f => f.id === id);
    }

    window.toggleFavorite = function (id, name, path) {
        let favs = getFavs();
        if (isFav(id)) {
            favs = favs.filter(f => f.id !== id);
            showToast('⭐ Removed from favorites', 2000);
        } else {
            favs.unshift({ id, name, path, ts: Date.now() });
            showToast('⭐ Added to favorites!', 2000, 'success');
        }
        saveFavs(favs);
        updateFavIcons();
        renderFavPanel();
    };

    function updateFavIcons() {
        document.querySelectorAll('.fav-btn').forEach(btn => {
            const id = btn.dataset.id;
            btn.classList.toggle('fav-active', isFav(id));
            btn.title = isFav(id) ? 'Remove from Favorites' : 'Add to Favorites';
        });
    }

    function renderFavPanel() {
        const panel = document.getElementById('fav-panel');
        if (!panel) return;
        const favs = getFavs();
        if (!favs.length) {
            panel.innerHTML = '<div class="recent-empty">No favorites yet</div>';
            return;
        }
        panel.innerHTML = favs.map(f => `
            <div class="recent-item fav-item" data-path="${f.path}" data-id="${f.id}" data-name="${f.name}">
                <span class="recent-icon">⭐</span>
                <span class="recent-name">${f.name}</span>
                <span class="fav-remove" data-id="${f.id}" title="Remove">✕</span>
            </div>
        `).join('');

        panel.querySelectorAll('.fav-item').forEach(el => {
            el.addEventListener('click', function (e) {
                if (e.target.classList.contains('fav-remove')) {
                    toggleFavorite(e.target.dataset.id, '', '');
                    return;
                }
                const name = this.dataset.name.toLowerCase();
                const rawPath = this.dataset.path + '/' + this.dataset.id;
                if (name.endsWith('.pdf') || name.endsWith('.epub'))
                    window.open('/pdf-viewer?path=' + encodeURIComponent(rawPath), '_blank');
                else window.open('/file?path=' + rawPath, '_blank');
            });
        });
    }

    // Inject fav buttons when directory renders
    const observer = new MutationObserver(() => {
        document.querySelectorAll('.directory-item:not([data-fav-checked])').forEach(item => {
            item.setAttribute('data-fav-checked', '1');
            const id = item.dataset.id;
            const name = item.dataset.name || item.querySelector('.item-title')?.textContent || '';
            const path = item.dataset.path || '';
            if (!id) return;

            const actions = item.querySelector('.item-actions');
            if (!actions) return;

            const favBtn = document.createElement('button');
            favBtn.className = 'fav-btn' + (isFav(id) ? ' fav-active' : '');
            favBtn.dataset.id = id;
            favBtn.title = isFav(id) ? 'Remove from Favorites' : 'Add to Favorites';
            favBtn.innerHTML = '⭐';
            favBtn.style.cssText = 'background:none;border:none;cursor:pointer;font-size:1rem;opacity:0.5;transition:opacity 0.2s,transform 0.2s;padding:4px;border-radius:6px;';
            favBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                toggleFavorite(id, name, path);
            });
            favBtn.addEventListener('mouseenter', () => { favBtn.style.opacity = '1'; favBtn.style.transform = 'scale(1.2)'; });
            favBtn.addEventListener('mouseleave', () => { favBtn.style.opacity = isFav(id) ? '1' : '0.5'; favBtn.style.transform = ''; });
            if (isFav(id)) favBtn.style.opacity = '1';

            actions.insertBefore(favBtn, actions.firstChild);
        });
    });

    document.addEventListener('DOMContentLoaded', () => {
        // Add Favorites section to sidebar
        const sidebarFooter = document.querySelector('.sidebar-footer');
        if (sidebarFooter) {
            const favSection = document.createElement('div');
            favSection.className = 'recent-panel-wrap';
            favSection.style.marginTop = '12px';
            favSection.innerHTML = '<div class="recent-panel-title">⭐ Favorites</div><div id="fav-panel"></div>';
            sidebarFooter.insertBefore(favSection, sidebarFooter.firstChild);
        }

        renderFavPanel();

        const dirData = document.getElementById('directory-data');
        if (dirData) observer.observe(dirData, { childList: true, subtree: true });
        const gridView = document.getElementById('grid-view');
        if (gridView) observer.observe(gridView, { childList: true, subtree: true });
    });
})();


// ── 3. WATCH HISTORY & VIDEO RESUME ─────────────────────────
(function () {
    const HISTORY_KEY = 'bd_watch_history';
    const MAX_HISTORY = 30;

    window.saveVideoProgress = function (videoId, progress, duration, name) {
        if (!videoId || !duration || duration < 10) return;
        let hist = getHistory();
        const pct = progress / duration;
        // Don't save if < 2% or > 97% (not started / finished)
        if (pct < 0.02 || pct > 0.97) return;
        hist = hist.filter(h => h.id !== videoId);
        hist.unshift({ id: videoId, progress, duration, name: name || 'Video', ts: Date.now() });
        if (hist.length > MAX_HISTORY) hist.pop();
        localStorage.setItem(HISTORY_KEY, JSON.stringify(hist));
    };

    window.getVideoResume = function (videoId) {
        const item = getHistory().find(h => h.id === videoId);
        return item ? item.progress : null;
    };

    function getHistory() {
        try { return JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]'); }
        catch { return []; }
    }

    window.clearWatchHistory = function () {
        localStorage.removeItem(HISTORY_KEY);
        showToast('🗑️ Watch history cleared', 2000);
    };

    // Show "In Progress" badge on files that have a saved position
    document.addEventListener('DOMContentLoaded', () => {
        const hist = getHistory();
        if (!hist.length) return;
        const histIds = new Set(hist.map(h => h.id));

        // Observe directory for file items and add progress badge
        const observer = new MutationObserver(() => {
            document.querySelectorAll('.file-item:not([data-hist-checked])').forEach(item => {
                item.setAttribute('data-hist-checked', '1');
                const id = item.dataset.id;
                if (!id || !histIds.has(id)) return;
                const histItem = hist.find(h => h.id === id);
                if (!histItem) return;
                const pct = Math.round((histItem.progress / histItem.duration) * 100);

                const titleEl = item.querySelector('.item-title');
                if (titleEl) {
                    const badge = document.createElement('span');
                    badge.className = 'watch-progress-badge';
                    badge.textContent = `▶ ${pct}%`;
                    badge.title = `Resume from ${formatSeconds(histItem.progress)}`;
                    titleEl.appendChild(badge);
                }
            });
        });

        const dirData = document.getElementById('directory-data');
        if (dirData) observer.observe(dirData, { childList: true, subtree: true });
    });

    function formatSeconds(s) {
        const h = Math.floor(s / 3600);
        const m = Math.floor((s % 3600) / 60);
        const sec = Math.floor(s % 60);
        return h > 0 ? `${h}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}` : `${m}:${String(sec).padStart(2,'0')}`;
    }
})();


// ── 4. FILE NOTES ────────────────────────────────────────────
(function () {
    const NOTES_KEY = 'bd_file_notes';

    function getNotes() {
        try { return JSON.parse(localStorage.getItem(NOTES_KEY) || '{}'); }
        catch { return {}; }
    }

    function saveNote(id, text) {
        const notes = getNotes();
        if (text.trim()) notes[id] = { text: text.trim(), ts: Date.now() };
        else delete notes[id];
        localStorage.setItem(NOTES_KEY, JSON.stringify(notes));
    }

    window.showNoteModal = function (id, name) {
        const notes = getNotes();
        const existing = notes[id]?.text || '';

        const overlay = document.createElement('div');
        overlay.className = 'modal';
        overlay.style.cssText = 'opacity:1;z-index:1100;';
        overlay.innerHTML = `
            <div class="modal-content">
                <div class="modal-header">
                    <h3>📝 Notes</h3>
                    <p>${name}</p>
                </div>
                <div class="modal-body">
                    <div class="input-group">
                        <label>Your Notes</label>
                        <textarea id="note-textarea" placeholder="Add your notes, thoughts, or reminders about this file…"
                            style="width:100%;min-height:120px;padding:10px;border-radius:10px;border:1.5px solid #e2e8f0;font-size:0.95rem;resize:vertical;font-family:inherit;">${existing}</textarea>
                    </div>
                </div>
                <div class="modal-footer">
                    <button class="btn btn-secondary" id="note-cancel">Cancel</button>
                    <button class="btn btn-primary" id="note-save">Save Note</button>
                    ${existing ? '<button class="btn btn-danger" id="note-clear">Clear</button>' : ''}
                </div>
            </div>`;

        document.body.appendChild(overlay);
        document.getElementById('note-textarea').focus();

        document.getElementById('note-cancel').onclick = () => overlay.remove();
        document.getElementById('note-save').onclick = () => {
            saveNote(id, document.getElementById('note-textarea').value);
            showToast('📝 Note saved!', 2000, 'success');
            overlay.remove();
            updateNoteIcons();
        };
        const clearBtn = document.getElementById('note-clear');
        if (clearBtn) clearBtn.onclick = () => {
            saveNote(id, '');
            showToast('📝 Note cleared', 2000);
            overlay.remove();
            updateNoteIcons();
        };
        overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
    };

    function hasNote(id) {
        const notes = getNotes();
        return !!notes[id]?.text;
    }

    function updateNoteIcons() {
        document.querySelectorAll('.note-btn').forEach(btn => {
            const id = btn.dataset.id;
            btn.classList.toggle('note-active', hasNote(id));
            btn.style.opacity = hasNote(id) ? '1' : '0.4';
        });
    }

    // Inject note buttons on directory items
    const observer = new MutationObserver(() => {
        document.querySelectorAll('.directory-item:not([data-note-checked])').forEach(item => {
            item.setAttribute('data-note-checked', '1');
            const id = item.dataset.id;
            const name = item.dataset.name || item.querySelector('.item-title')?.textContent || '';
            if (!id) return;

            const actions = item.querySelector('.item-actions');
            if (!actions) return;

            const noteBtn = document.createElement('button');
            noteBtn.className = 'note-btn' + (hasNote(id) ? ' note-active' : '');
            noteBtn.dataset.id = id;
            noteBtn.title = 'Add/Edit Note';
            noteBtn.innerHTML = '📝';
            noteBtn.style.cssText = 'background:none;border:none;cursor:pointer;font-size:0.9rem;opacity:' + (hasNote(id) ? '1' : '0.4') + ';transition:opacity 0.2s;padding:4px;border-radius:6px;';
            noteBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                showNoteModal(id, name);
            });
            noteBtn.addEventListener('mouseenter', () => { noteBtn.style.opacity = '1'; });
            noteBtn.addEventListener('mouseleave', () => { noteBtn.style.opacity = hasNote(id) ? '1' : '0.4'; });

            actions.insertBefore(noteBtn, actions.firstChild);
        });
    });

    document.addEventListener('DOMContentLoaded', () => {
        const dirData = document.getElementById('directory-data');
        if (dirData) observer.observe(dirData, { childList: true, subtree: true });
        const gridView = document.getElementById('grid-view');
        if (gridView) observer.observe(gridView, { childList: true, subtree: true });
    });
})();


// ── 5. DOWNLOAD PROGRESS TRACKER BAR ─────────────────────────
(function () {
    let activeDownloads = {};

    function getOrCreateBar() {
        let bar = document.getElementById('dl-tracker-bar');
        if (!bar) {
            bar = document.createElement('div');
            bar.id = 'dl-tracker-bar';
            bar.innerHTML = `
                <div class="dl-tracker-inner">
                    <span class="dl-tracker-icon">⬇️</span>
                    <span id="dl-tracker-text">No active downloads</span>
                    <div id="dl-tracker-list"></div>
                    <button id="dl-tracker-close" title="Hide">✕</button>
                </div>`;
            document.body.appendChild(bar);
            document.getElementById('dl-tracker-close').onclick = () => bar.classList.remove('dl-tracker-visible');
        }
        return bar;
    }

    window.trackDownload = function (id, name, url) {
        const bar = getOrCreateBar();
        bar.classList.add('dl-tracker-visible');
        activeDownloads[id] = { name, status: 'Starting…' };
        updateTrackerUI();

        // Simulate tracking (real XHR progress tracking)
        const xhr = new XMLHttpRequest();
        xhr.open('GET', url, true);
        xhr.responseType = 'blob';

        xhr.addEventListener('progress', (e) => {
            if (e.lengthComputable) {
                const pct = Math.round((e.loaded / e.total) * 100);
                activeDownloads[id] = { name, status: pct + '%', pct };
            } else {
                activeDownloads[id] = { name, status: 'Downloading…' };
            }
            updateTrackerUI();
        });

        xhr.addEventListener('load', () => {
            const blobUrl = URL.createObjectURL(xhr.response);
            const a = document.createElement('a');
            a.href = blobUrl;
            a.download = name;
            a.click();
            URL.revokeObjectURL(blobUrl);
            delete activeDownloads[id];
            updateTrackerUI();
            showToast(`✅ Downloaded: ${name}`, 3000, 'success');
        });

        xhr.addEventListener('error', () => {
            delete activeDownloads[id];
            updateTrackerUI();
            showToast(`❌ Download failed: ${name}`, 4000, 'error');
        });

        xhr.send();
    };

    function updateTrackerUI() {
        const bar = document.getElementById('dl-tracker-bar');
        if (!bar) return;
        const list = document.getElementById('dl-tracker-list');
        const text = document.getElementById('dl-tracker-text');
        const count = Object.keys(activeDownloads).length;

        if (count === 0) {
            text.textContent = 'All downloads complete';
            list.innerHTML = '';
            setTimeout(() => bar.classList.remove('dl-tracker-visible'), 2000);
            return;
        }

        text.textContent = count + ' download' + (count > 1 ? 's' : '') + ' in progress';
        list.innerHTML = Object.values(activeDownloads).map(d => `
            <div class="dl-item">
                <span class="dl-name">${d.name.length > 30 ? d.name.slice(0, 30) + '…' : d.name}</span>
                <span class="dl-status">${d.status}</span>
                ${d.pct !== undefined ? `<div class="dl-mini-bar"><div class="dl-mini-filled" style="width:${d.pct}%"></div></div>` : ''}
            </div>`).join('');
    }
})();


// ── 6. SEARCH FILTER PILLS ───────────────────────────────────
(function () {
    const TYPES = [
        { label: 'All', value: '', icon: '📁' },
        { label: 'Videos', value: 'video', icon: '🎬' },
        { label: 'PDFs', value: 'pdf', icon: '📄' },
        { label: 'Images', value: 'image', icon: '🖼️' },
        { label: 'Audio', value: 'audio', icon: '🎵' },
    ];

    let activeFilter = '';

    const VIDEO_EXTS = ['.mp4','.mkv','.webm','.mov','.avi','.ts','.m4v'];
    const PDF_EXTS   = ['.pdf','.epub'];
    const IMG_EXTS   = ['.jpg','.jpeg','.png','.gif','.webp','.bmp','.svg'];
    const AUDIO_EXTS = ['.mp3','.wav','.flac','.aac','.ogg','.m4a','.opus'];

    function matchesFilter(name) {
        if (!activeFilter) return true;
        const n = name.toLowerCase();
        if (activeFilter === 'video') return VIDEO_EXTS.some(e => n.endsWith(e));
        if (activeFilter === 'pdf')   return PDF_EXTS.some(e => n.endsWith(e));
        if (activeFilter === 'image') return IMG_EXTS.some(e => n.endsWith(e));
        if (activeFilter === 'audio') return AUDIO_EXTS.some(e => n.endsWith(e));
        return true;
    }

    function applyFilter() {
        document.querySelectorAll('.directory-item.file-item').forEach(item => {
            const name = item.dataset.name || item.querySelector('.item-title')?.textContent || '';
            item.style.display = matchesFilter(name) ? '' : 'none';
        });
        // Always show folders
        document.querySelectorAll('.directory-item.folder-item').forEach(item => {
            item.style.display = activeFilter ? 'none' : '';
        });
    }

    document.addEventListener('DOMContentLoaded', () => {
        // Search toggle — icon button shows/hides search bar
        const toggleBtn = document.getElementById('search-toggle-btn');
        const searchCont = document.getElementById('search-container');
        const searchInput = document.getElementById('file-search');

        if (toggleBtn && searchCont) {
            toggleBtn.addEventListener('click', () => {
                const collapsed = searchCont.classList.toggle('collapsed');
                if (!collapsed && searchInput) {
                    setTimeout(() => searchInput.focus(), 50);
                }
            });
            // Close on Escape
            document.addEventListener('keydown', e => {
                if (e.key === 'Escape' && !searchCont.classList.contains('collapsed')) {
                    searchCont.classList.add('collapsed');
                    if (searchInput) searchInput.value = '';
                }
            });
        }
    });
})();


// ── 7. COPY LINK SHORTCUT IN ACTIONS ─────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    const observer = new MutationObserver(() => {
        document.querySelectorAll('.directory-item.file-item:not([data-copylink-checked])').forEach(item => {
            item.setAttribute('data-copylink-checked', '1');
            const id = item.dataset.id;
            const path = item.dataset.path;
            if (!id) return;

            const moreMenu = document.getElementById(`more-option-${id}`);
            if (!moreMenu) return;

            if (moreMenu.querySelector('.copy-link-item')) return;

            const copyLinkEl = document.createElement('div');
            copyLinkEl.className = 'more-options-item copy-link-item';
            copyLinkEl.innerHTML = `
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/>
                    <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>
                </svg>
                <span>Copy Direct Link</span>`;
            copyLinkEl.addEventListener('click', (e) => {
                e.stopPropagation();
                const link = window.location.origin + '/file?path=' + path + '/' + id;
                navigator.clipboard.writeText(link).then(() => {
                    showToast('🔗 Link copied to clipboard!', 2500, 'success');
                }).catch(() => {
                    showToast('❌ Could not copy link', 2500, 'error');
                });
            });

            moreMenu.appendChild(copyLinkEl);
        });
    });

    const dirData = document.getElementById('directory-data');
    if (dirData) observer.observe(dirData, { childList: true, subtree: true });
});
