function openFolder() {
    const folderEl = this
    let path = (getCurrentPath() + '/' + this.getAttribute('data-id') + '/').replaceAll('//', '/')

    const auth = getFolderAuthFromPath()
    if (auth) {
        path = path + '&auth=' + auth
    }

    // Use data-path + data-id to build the full folder path — must match what lockFolder uses
    const folderDataPath = folderEl.getAttribute('data-path') || getCurrentPath()
    const folderDataId   = folderEl.getAttribute('data-id')
    const rawPath = (folderDataPath + '/' + folderDataId).replaceAll('//', '/')
    checkAndPromptFolderLock(rawPath).then(allowed => {
        if (allowed) window.location.href = `/?path=${path}`
    })
}

function openFile() {
    const fileName = this.getAttribute('data-name').toLowerCase()
    let path = '/file?path=' + this.getAttribute('data-path') + '/' + this.getAttribute('data-id')
    const rawPath = this.getAttribute('data-path') + '/' + this.getAttribute('data-id')

    // PDF / EPUB — built-in viewer
    if (fileName.endsWith('.pdf') || fileName.endsWith('.epub')) {
        window.open('/pdf-viewer?path=' + encodeURIComponent(rawPath), '_blank')
        return
    }

    // Image files — image viewer
    const imageExts = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg', '.ico', '.avif', '.tiff', '.tif']
    if (imageExts.some(ext => fileName.endsWith(ext))) {
        window.open('/image-viewer?path=' + encodeURIComponent(rawPath), '_blank')
        return
    }

    // Audio files — audio player
    const audioExts = ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.opus', '.wma', '.aiff', '.alac']
    if (audioExts.some(ext => fileName.endsWith(ext))) {
        window.open('/audio-player?path=' + encodeURIComponent(rawPath), '_blank')
        return
    }

    // Video files — player selection
    if (fileName.endsWith('.mp4') || fileName.endsWith('.mkv') || fileName.endsWith('.webm') || fileName.endsWith('.mov') || fileName.endsWith('.avi') || fileName.endsWith('.ts') || fileName.endsWith('.ogv') || fileName.endsWith('.m4v') || fileName.endsWith('.flv') || fileName.endsWith('.3gp') || fileName.endsWith('.mpg') || fileName.endsWith('.mpeg')) {
        showPlayerSelectionModal(path, rawPath)
        return
    }

    window.open(path, '_blank')
}

// Player Selection Modal
function showPlayerSelectionModal(filePath, rawPath) {
    rawPath = rawPath || filePath.replace('/file?path=', '');
    const modal = document.createElement('div');
    modal.className = 'modal';
    modal.style.zIndex = '1000';
    modal.style.opacity = '1';
    
    modal.innerHTML = `
        <div class="modal-content">
            <div class="modal-header">
                <h3>🎬 Choose Video Player</h3>
                <p>Select your preferred video player for the best experience</p>
            </div>
            <div class="modal-body">
                <div class="player-options">
                    <div class="player-option" onclick="openStandardPlayer('${filePath}')">
                        <div class="player-icon standard">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polygon points="5 3 19 12 5 21 5 3"/>
                            </svg>
                        </div>
                        <div class="player-info">
                            <h4>🎥 Standard Player</h4>
                            <p>Full-featured player with all controls and high quality</p>
                            <span class="player-badge">Best for: High-speed internet</span>
                        </div>
                    </div>
                    
                    <div class="player-option" onclick="openFastPlayer('${filePath}')">
                        <div class="player-icon fast">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polygon points="13 19 22 12 13 5 13 19"/>
                                <polygon points="2 19 11 12 2 5 2 19"/>
                            </svg>
                        </div>
                        <div class="player-info">
                            <h4>⚡ Fast Player</h4>
                            <p>Optimized for slow connections with adaptive streaming</p>
                            <span class="player-badge fast">Best for: Slow internet, mobile data</span>
                        </div>
                    </div>

                    <div class="player-option" onclick="openAirPlayer('${filePath}', '${rawPath}')">
                        <div class="player-icon" style="background:linear-gradient(135deg,#00d4ff22,#00d4ff44);border:1.5px solid #00d4ff55">
                            <svg viewBox="0 0 24 24" fill="none" stroke="#00d4ff" stroke-width="2">
                                <path d="M12 2L2 7l10 5 10-5-10-5z"/>
                                <path d="M2 17l10 5 10-5"/>
                                <path d="M2 12l10 5 10-5"/>
                            </svg>
                        </div>
                        <div class="player-info">
                            <h4 style="color:#00d4ff">✈ AirPlayer</h4>
                            <p>Ultra-fast streaming with dual audio & quality switching</p>
                            <span class="player-badge" style="background:#00d4ff22;color:#00d4ff;border:1px solid #00d4ff44">Best for: All connections · Dual Audio · Multi-quality</span>
                        </div>
                    </div>
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" onclick="closePlayerModal()">Cancel</button>
            </div>
        </div>
    `;
    
    // Add styles for player selection
    const style = document.createElement('style');
    style.textContent = `
        .player-options {
            display: flex;
            flex-direction: column;
            gap: var(--space-4);
        }
        
        .player-option {
            display: flex;
            align-items: center;
            gap: var(--space-4);
            padding: var(--space-5);
            border: 2px solid var(--secondary-200);
            border-radius: var(--radius-xl);
            cursor: pointer;
            transition: all var(--transition-fast);
            background: var(--secondary-50);
        }
        
        .player-option:hover {
            border-color: var(--primary-500);
            background: var(--primary-50);
            transform: translateY(-2px);
            box-shadow: var(--shadow-lg);
        }
        
        .player-icon {
            width: 60px;
            height: 60px;
            border-radius: var(--radius-xl);
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
        }
        
        .player-icon.standard {
            background: linear-gradient(135deg, var(--primary-500), var(--primary-600));
        }
        
        .player-icon.fast {
            background: linear-gradient(135deg, var(--success-500), var(--success-600));
        }
        
        .player-icon svg {
            width: 28px;
            height: 28px;
            stroke: white;
        }
        
        .player-info {
            flex: 1;
        }
        
        .player-info h4 {
            font-size: 1.1rem;
            font-weight: 700;
            color: var(--secondary-900);
            margin-bottom: var(--space-2);
        }
        
        .player-info p {
            color: var(--secondary-600);
            margin-bottom: var(--space-3);
            line-height: 1.5;
        }
        
        .player-badge {
            display: inline-block;
            padding: var(--space-1) var(--space-3);
            background: var(--primary-100);
            color: var(--primary-700);
            border-radius: var(--radius-full);
            font-size: 0.8rem;
            font-weight: 600;
        }
        
        .player-badge.fast {
            background: var(--success-100);
            color: var(--success-700);
        }
        
        @media (max-width: 768px) {
            .player-option {
                flex-direction: column;
                text-align: center;
                gap: var(--space-3);
            }
            
            .player-icon {
                width: 50px;
                height: 50px;
            }
            
            .player-icon svg {
                width: 24px;
                height: 24px;
            }
        }
    `;
    
    document.head.appendChild(style);
    document.body.appendChild(modal);
    
    // Show background blur
    document.getElementById('bg-blur').style.zIndex = '999';
    document.getElementById('bg-blur').style.opacity = '0.5';
}

function openStandardPlayer(filePath) {
    const streamPath = '/stream?url=' + getRootUrl() + filePath;
    window.open(streamPath, '_blank');
    closePlayerModal();
}

function openFastPlayer(filePath) {
    const fastPlayerPath = '/fast-player?url=' + getRootUrl() + filePath;
    window.open(fastPlayerPath, '_blank');
    closePlayerModal();
}

function openAirPlayer(filePath, rawPath) {
    // filePath = /file?path=/T8RN0O/R2ZU09
    // rawPath  = /T8RN0O/R2ZU09 (the actual drive path we need)
    const drivePath = rawPath || filePath.replace('/file?path=', '');
    const airPlayerPath = '/air-player?url=' + getRootUrl() + filePath + '&path=' + encodeURIComponent(drivePath);
    window.open(airPlayerPath, '_blank');
    closePlayerModal();
}

function closePlayerModal() {
    const modal = document.querySelector('.modal[style*="z-index: 1000"]');
    if (modal) {
        modal.remove();
    }
    
    // Hide background blur
    document.getElementById('bg-blur').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('bg-blur').style.zIndex = '-1';
    }, 300);
}

// File More Button Handler Start

function openMoreButton(div) {
    const id = div.getAttribute('data-id')
    const moreDiv = document.getElementById(`more-option-${id}`)

    const rect = div.getBoundingClientRect();
    const x = rect.left + window.scrollX - 40;
    const y = rect.top + window.scrollY;

    moreDiv.style.zIndex = 2
    moreDiv.style.opacity = 1
    moreDiv.style.left = `${x}px`
    moreDiv.style.top = `${y}px`

    const isTrash = getCurrentPath().includes('/trash')

    moreDiv.querySelector('.more-options-focus').focus()
    moreDiv.querySelector('.more-options-focus').addEventListener('blur', closeMoreBtnFocus);
    moreDiv.querySelector('.more-options-focus').addEventListener('focusout', closeMoreBtnFocus);
    if (!isTrash) {
        moreDiv.querySelector(`#rename-${id}`).addEventListener('click', renameFileFolder)
        moreDiv.querySelector(`#trash-${id}`).addEventListener('click', trashFileFolder)
        moreDiv.querySelector(`#move-${id}`).addEventListener('click', moveFileFolder)
        moreDiv.querySelector(`#copy-${id}`).addEventListener('click', copyFileFolder)
        try {
            moreDiv.querySelector(`#share-${id}`).addEventListener('click', shareFile)
        }
        catch { }
        try {
            moreDiv.querySelector(`#encode-${id}`).addEventListener('click', encodeVideo)
        }
        catch { }
        try {
            moreDiv.querySelector(`#folder-share-${id}`).addEventListener('click', shareFolder)
        }
        catch { }
        try {
            moreDiv.querySelector(`#lock-folder-${id}`).addEventListener('click', lockFolder)
        }
        catch { }
    }
    else {
        moreDiv.querySelector(`#restore-${id}`).addEventListener('click', restoreFileFolder)
        moreDiv.querySelector(`#delete-${id}`).addEventListener('click', deleteFileFolder)
    }
}

function closeMoreBtnFocus() {
    const moreDiv = this.parentElement
    moreDiv.style.opacity = '0'
    setTimeout(() => {
        moreDiv.style.zIndex = '-1'
    }, 300)
}

// Rename File Folder Start
function renameFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    console.log(id)

    document.getElementById('rename-name').value = this.parentElement.getAttribute('data-name');
    document.getElementById('bg-blur').style.zIndex = '2';
    document.getElementById('bg-blur').style.opacity = '0.1';

    document.getElementById('rename-file-folder').style.zIndex = '3';
    document.getElementById('rename-file-folder').style.opacity = '1';
    document.getElementById('rename-file-folder').setAttribute('data-id', id);
    setTimeout(() => {
        document.getElementById('rename-name').focus();
    }, 300)
}

document.getElementById('rename-cancel').addEventListener('click', () => {
    document.getElementById('rename-name').value = '';
    document.getElementById('bg-blur').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('bg-blur').style.zIndex = '-1';
    }, 300)
    document.getElementById('rename-file-folder').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('rename-file-folder').style.zIndex = '-1';
    }, 300)
});

document.getElementById('rename-create').addEventListener('click', async () => {
    const name = document.getElementById('rename-name').value;
    if (name === '') {
        alert('Name cannot be empty')
        return
    }

    const id = document.getElementById('rename-file-folder').getAttribute('data-id')

    const path = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id

    const data = {
        'name': name,
        'path': path
    }

    const response = await postJson('/api/renameFileFolder', data)
    if (response.status === 'ok') {
        alert('File/Folder Renamed Successfully')
        window.location.reload();
    } else {
        alert('Failed to rename file/folder')
        window.location.reload();
    }
});

// Rename File Folder End

// Move File Folder Start
function moveFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    const itemPath = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    const itemName = document.getElementById(`more-option-${id}`).getAttribute('data-name')
    
    // Close the more options menu first
    closeMoreBtnFocus.call(this.parentElement.querySelector('.more-options-focus'))
    
    // Show move modal
    showMoveModal(itemPath, itemName)
}

// Copy File Folder Start
function copyFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    const itemPath = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    const itemName = document.getElementById(`more-option-${id}`).getAttribute('data-name')
    
    // Close the more options menu first
    closeMoreBtnFocus.call(this.parentElement.querySelector('.more-options-focus'))
    
    // Show copy modal
    showCopyModal(itemPath, itemName)
}

async function trashFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    console.log(id)
    const path = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    const data = {
        'path': path,
        'trash': true
    }
    const response = await postJson('/api/trashFileFolder', data)

    if (response.status === 'ok') {
        alert('File/Folder Sent to Archive Successfully')
        window.location.reload();
    } else {
        alert('Failed to Send File/Folder to Archive')
        window.location.reload();
    }
}

async function restoreFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    const path = this.getAttribute('data-path') + '/' + id
    const data = {
        'path': path,
        'trash': false
    }
    const response = await postJson('/api/trashFileFolder', data)

    if (response.status === 'ok') {
        alert('File/Folder Restored Successfully')
        window.location.reload();
    } else {
        alert('Failed to Restored File/Folder')
        window.location.reload();
    }
}

async function deleteFileFolder() {
    const id = this.getAttribute('id').split('-')[1]
    const path = this.getAttribute('data-path') + '/' + id
    const data = {
        'path': path
    }
    const response = await postJson('/api/deleteFileFolder', data)

    if (response.status === 'ok') {
        alert('File/Folder Deleted Successfully')
        window.location.reload();
    } else {
        alert('Failed to Delete File/Folder')
        window.location.reload();
    }
}

async function shareFile() {
    const fileName = this.parentElement.getAttribute('data-name').toLowerCase()
    const id = this.getAttribute('id').split('-')[1]
    const path = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    const root_url = getRootUrl()

    const imageExts = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg', '.ico', '.avif', '.tiff', '.tif']
    const audioExts = ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.opus', '.wma', '.aiff', '.alac']
    const videoExts = ['.mp4', '.mkv', '.webm', '.mov', '.avi', '.ts', '.ogv', '.m4v', '.flv', '.3gp', '.mpg', '.mpeg']

    let link
    if (fileName.endsWith('.pdf') || fileName.endsWith('.epub')) {
        link = `${root_url}/pdf-viewer?path=${encodeURIComponent(path)}`
    } else if (imageExts.some(ext => fileName.endsWith(ext))) {
        link = `${root_url}/image-viewer?path=${encodeURIComponent(path)}`
    } else if (audioExts.some(ext => fileName.endsWith(ext))) {
        link = `${root_url}/audio-player?path=${encodeURIComponent(path)}`
    } else if (videoExts.some(ext => fileName.endsWith(ext))) {
        link = `${root_url}/stream?url=${root_url}/file?path=${path}`
    } else {
        link = `${root_url}/file?path=${path}`
    }

    copyTextToClipboard(link)
}

async function encodeVideo() {
    const id = this.getAttribute('id').split('-')[1]
    const fileName = document.getElementById(`more-option-${id}`).getAttribute('data-name')
    const filePath = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    
    // Close the more options menu first
    closeMoreBtnFocus.call(this.parentElement.querySelector('.more-options-focus'))
    
    // Check if it's a video file
    const videoExtensions = ['.mp4', '.mkv', '.webm', '.mov', '.avi', '.ts', '.ogv', '.m4v', '.flv', '.wmv', '.3gp', '.mpg', '.mpeg']
    const isVideo = videoExtensions.some(ext => fileName.toLowerCase().endsWith(ext))
    
    if (!isVideo) {
        alert('This feature is only available for video files.')
        return
    }
    
    // Show encoding modal
    showVideoEncodingModal(filePath, fileName)
}

async function shareFolder() {
    const id = this.getAttribute('id').split('-')[2]
    console.log(id)
    let path = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    const root_url = getRootUrl()

    const auth = await getFolderShareAuth(path)
    path = path.slice(1)

    let link = `${root_url}/?path=/share_${path}&auth=${auth}`
    console.log(link)

    copyTextToClipboard(link)
}

// ── Folder Lock ────────────────────────────────────────────────────────────

function lockFolder() {
    const id   = this.getAttribute('id').split('-')[2]
    const path = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id
    showFolderLockModal(path)
}

function showFolderLockModal(folderPath) {
    // Remove any existing modal
    const existing = document.getElementById('folder-lock-modal')
    if (existing) existing.remove()

    const modal = document.createElement('div')
    modal.id = 'folder-lock-modal'
    modal.className = 'modal'
    modal.style.cssText = 'z-index:1000;opacity:1;'
    modal.innerHTML = `
        <div class="modal-content">
            <div class="modal-header">
                <h3>🔒 Lock Folder</h3>
                <p>Set or remove a password on this folder</p>
            </div>
            <div class="modal-body">
                <div id="lock-status-msg" style="font-size:.82rem;color:#94a3b8;margin-bottom:12px;"></div>
                <div class="input-group">
                    <label for="lock-folder-pass">Folder Password</label>
                    <input type="password" id="lock-folder-pass" placeholder="Enter folder password" autocomplete="new-password"/>
                </div>
                <div class="input-group" id="lock-confirm-group">
                    <label for="lock-folder-pass2">Confirm Password</label>
                    <input type="password" id="lock-folder-pass2" placeholder="Confirm password" autocomplete="new-password"/>
                </div>
            </div>
            <div class="modal-footer">
                <button id="lock-remove-btn" class="btn btn-danger" style="margin-right:auto">Remove Lock</button>
                <button id="lock-cancel-btn" class="btn btn-secondary">Cancel</button>
                <button id="lock-set-btn" class="btn btn-primary">Lock Folder</button>
            </div>
        </div>
    `
    document.body.appendChild(modal)

    const blur = document.getElementById('bg-blur')
    blur.style.zIndex = '999'
    blur.style.opacity = '0.5'

    function closeLockModal() {
        modal.remove()
        blur.style.opacity = '0'
        setTimeout(() => { blur.style.zIndex = '-1' }, 300)
    }

    document.getElementById('lock-cancel-btn').addEventListener('click', closeLockModal)

    // Check if already locked
    fetch(`/api/v2/folder-password/check/${encodeURIComponent(folderPath.replace(/^\//, ''))}`)
        .then(r => r.json())
        .then(data => {
            const statusEl = document.getElementById('lock-status-msg')
            if (data.protected) {
                statusEl.innerHTML = '🔒 <strong style="color:#f59e0b">This folder is currently locked.</strong> Set a new password to change it.'
            } else {
                statusEl.textContent = '🔓 This folder is not locked. Set a password to restrict access.'
                document.getElementById('lock-remove-btn').style.display = 'none'
            }
        }).catch(() => {})

    document.getElementById('lock-set-btn').addEventListener('click', async () => {
        const pass  = document.getElementById('lock-folder-pass').value
        const pass2 = document.getElementById('lock-folder-pass2').value
        if (!pass) { alert('Please enter a password.'); return }
        if (pass !== pass2) { alert('Passwords do not match.'); return }

        const res = await fetch('/api/v2/folder-password/set', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ admin_password: getPassword(), folder_path: folderPath, password: pass })
        })
        const data = await res.json()
        if (data.status === 'ok') {
            alert('✅ Folder locked successfully!')
            closeLockModal()
        } else {
            alert('❌ Failed to lock folder: ' + (data.message || data.status))
        }
    })

    document.getElementById('lock-remove-btn').addEventListener('click', async () => {
        if (!confirm('Remove the lock from this folder?')) return
        const res = await fetch('/api/v2/folder-password/remove', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ admin_password: getPassword(), folder_path: folderPath })
        })
        const data = await res.json()
        if (data.status === 'ok') {
            alert('✅ Folder lock removed.')
            closeLockModal()
        } else {
            alert('❌ Failed: ' + (data.message || data.status))
        }
    })
}

// ── Folder unlock prompt (shown when entering a locked folder) ─────────────

// Session-level cache of unlocked folder paths (survives page navigations via sessionStorage)
const UNLOCK_CACHE_KEY = 'bd_unlocked_folders'

function getUnlockedFolders() {
    try { return JSON.parse(sessionStorage.getItem(UNLOCK_CACHE_KEY) || '[]') }
    catch { return [] }
}

function markFolderUnlocked(folderPath) {
    const list = getUnlockedFolders()
    if (!list.includes(folderPath)) {
        list.push(folderPath)
        sessionStorage.setItem(UNLOCK_CACHE_KEY, JSON.stringify(list))
    }
}

function isFolderCachedUnlocked(folderPath) {
    const list = getUnlockedFolders()
    // Also check parent paths (so unlocking a parent unlocks children)
    return list.some(p => folderPath === p || folderPath.startsWith(p + '/'))
}

async function checkAndPromptFolderLock(folderPath) {
    try {
        // 1. Check client-side session cache first — avoids re-prompting on page nav
        if (isFolderCachedUnlocked(folderPath)) return true

        // 2. Ask server if this folder is protected
        const res  = await fetch(`/api/v2/folder-password/check/${encodeURIComponent(folderPath.replace(/^\//, ''))}`)
        const data = await res.json()
        if (!data.protected) return true  // not locked, proceed

        // 3. Show password prompt
        return new Promise(resolve => {
            const modal = document.createElement('div')
            modal.className = 'modal'
            modal.style.cssText = 'z-index:1000;opacity:1;'
            modal.innerHTML = `
                <div class="modal-content">
                    <div class="modal-header">
                        <h3>🔒 Locked Folder</h3>
                        <p>This folder is password protected. Enter the folder password to access it.</p>
                    </div>
                    <div class="modal-body">
                        <div class="input-group">
                            <label for="unlock-pass">Folder Password</label>
                            <input type="password" id="unlock-pass" placeholder="Enter folder password" autocomplete="off"/>
                        </div>
                        <div id="unlock-error" style="color:#ef4444;font-size:0.85rem;margin-top:8px;display:none;">
                            ❌ Wrong password. Please try again.
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button id="unlock-cancel" class="btn btn-secondary">Cancel</button>
                        <button id="unlock-submit" class="btn btn-primary">Unlock</button>
                    </div>
                </div>
            `
            document.body.appendChild(modal)
            const blur = document.getElementById('bg-blur')
            blur.style.zIndex = '999'; blur.style.opacity = '0.5'
            setTimeout(() => document.getElementById('unlock-pass').focus(), 200)

            function close(result) {
                modal.remove()
                blur.style.opacity = '0'
                setTimeout(() => { blur.style.zIndex = '-1' }, 300)
                resolve(result)
            }

            async function attemptUnlock() {
                const pass = document.getElementById('unlock-pass').value
                if (!pass) { alert('Please enter the password.'); return }
                const submitBtn = document.getElementById('unlock-submit')
                submitBtn.textContent = 'Verifying…'
                submitBtn.disabled = true
                try {
                    const r = await fetch('/api/v2/folder-password/verify', {
                        method: 'POST',
                        headers: {'Content-Type':'application/json'},
                        body: JSON.stringify({ folder_path: folderPath, password: pass })
                    })
                    const d = await r.json()
                    if (d.status === 'ok') {
                        // Cache in sessionStorage so re-visiting same folder in this browser tab won't re-prompt
                        markFolderUnlocked(folderPath)
                        close(true)
                    } else {
                        const errEl = document.getElementById('unlock-error')
                        if (errEl) errEl.style.display = 'block'
                        document.getElementById('unlock-pass').value = ''
                        document.getElementById('unlock-pass').focus()
                        submitBtn.textContent = 'Unlock'
                        submitBtn.disabled = false
                    }
                } catch (err) {
                    submitBtn.textContent = 'Unlock'
                    submitBtn.disabled = false
                    alert('Network error — please try again.')
                }
            }

            document.getElementById('unlock-cancel').addEventListener('click', () => close(false))
            document.getElementById('unlock-submit').addEventListener('click', attemptUnlock)
            document.getElementById('unlock-pass').addEventListener('keydown', e => {
                if (e.key === 'Enter') attemptUnlock()
            })
        })
    } catch (e) {
        return true // on error, allow access
    }
}

// File More Button Handler  End