function openFolder() {
    let path = (getCurrentPath() + '/' + this.getAttribute('data-id') + '/').replaceAll('//', '/')

    const auth = getFolderAuthFromPath()
    if (auth) {
        path = path + '&auth=' + auth
    }
    window.location.href = `/?path=${path}`
}

function openFile() {
    const fileName = this.getAttribute('data-name').toLowerCase()
    let path = '/file?path=' + this.getAttribute('data-path') + '/' + this.getAttribute('data-id')

    // Check if it's a PDF or EPUB file — open in the built-in viewer
    if (fileName.endsWith('.pdf') || fileName.endsWith('.epub')) {
        const viewerPath = '/pdf-viewer?path=' + this.getAttribute('data-path') + '/' + this.getAttribute('data-id')
        window.open(viewerPath, '_blank')
        return
    }

    // Check if it's a video file
    if (fileName.endsWith('.mp4') || fileName.endsWith('.mkv') || fileName.endsWith('.webm') || fileName.endsWith('.mov') || fileName.endsWith('.avi') || fileName.endsWith('.ts') || fileName.endsWith('.ogv')) {
        // Show player selection modal
        showPlayerSelectionModal(path)
        return
    }

    window.open(path, '_blank')
}

// Player Selection Modal
function showPlayerSelectionModal(filePath) {
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

    let link
    if (fileName.endsWith('.pdf') || fileName.endsWith('.epub')) {
        // Share viewer link for PDF and EPUB
        link = `${root_url}/pdf-viewer?path=${path}`
    } else if (fileName.endsWith('.mp4') || fileName.endsWith('.mkv') || fileName.endsWith('.webm') || fileName.endsWith('.mov') || fileName.endsWith('.avi') || fileName.endsWith('.ts') || fileName.endsWith('.ogv')) {
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

// File More Button Handler  End