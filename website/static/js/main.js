function showDirectory(data) {
    data = data['contents']
    document.getElementById('directory-data').innerHTML = ''
    const isTrash = getCurrentPath().startsWith('/trash')

    let html = ''

    // The data is already sorted by the server, so we just need to render it
    for (const [key, item] of Object.entries(data)) {
        if (item.type === 'folder') {
            html += `
                <div data-path="${item.path}" data-id="${item.id}" class="directory-item folder-item">
                    <div class="item-name">
                        <div class="item-icon folder">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>
                            </svg>
                        </div>
                        <div class="item-text">
                            <div class="item-title">${item.name}</div>
                        </div>
                    </div>
                    <div class="item-size">-</div>
                    <div class="item-duration">-</div>
                    <div class="item-actions">
                        <button data-id="${item.id}" class="more-btn">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <circle cx="12" cy="12" r="1"/>
                                <circle cx="19" cy="12" r="1"/>
                                <circle cx="5" cy="12" r="1"/>
                            </svg>
                        </button>
                    </div>
                </div>
            `

            if (isTrash) {
                html += `
                    <div data-path="${item.path}" id="more-option-${item.id}" data-name="${item.name}" class="more-options">
                        <input class="more-options-focus" readonly="readonly" style="height:0;width:0;border:none;position:absolute">
                        <div id="restore-${item.id}" data-path="${item.path}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8"/>
                                <path d="M21 3v5h-5"/>
                                <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16"/>
                                <path d="M8 16H3v5"/>
                            </svg>
                            <span>Restore</span>
                        </div>
                        <div id="delete-${item.id}" data-path="${item.path}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polyline points="3,6 5,6 21,6"/>
                                <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                                <line x1="10" y1="11" x2="10" y2="17"/>
                                <line x1="14" y1="11" x2="14" y2="17"/>
                            </svg>
                            <span>Delete Permanently</span>
                        </div>
                    </div>
                `
            } else {
                html += `
                    <div data-path="${item.path}" id="more-option-${item.id}" data-name="${item.name}" class="more-options">
                        <input class="more-options-focus" readonly="readonly" style="height:0;width:0;border:none;position:absolute">
                        <div id="rename-${item.id}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
                                <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
                            </svg>
                            <span>Rename</span>
                        </div>
                        <div id="move-${item.id}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                                <polyline points="14,2 14,8 20,8"/>
                                <line x1="16" y1="13" x2="8" y2="13"/>
                                <line x1="16" y1="17" x2="8" y2="17"/>
                                <polyline points="10,9 9,9 8,9"/>
                            </svg>
                            <span>Move</span>
                        </div>
                        <div id="copy-${item.id}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
                                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
                            </svg>
                            <span>Copy</span>
                        </div>
                        <div id="trash-${item.id}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polyline points="3,6 5,6 21,6"/>
                                <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                            </svg>
                            <span>Archive</span>
                        </div>
                        <div id="folder-share-${item.id}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"/>
                                <polyline points="16,6 12,2 8,6"/>
                                <line x1="12" y1="2" x2="12" y2="15"/>
                            </svg>
                            <span>Share Module</span>
                        </div>
                        <div id="lock-folder-${item.id}" class="more-options-item" data-path="${item.path}/${item.id}">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/>
                                <path d="M7 11V7a5 5 0 0 1 10 0v4"/>
                            </svg>
                            <span>Lock Folder</span>
                        </div>
                    </div>
                `
            }
        } else if (item.type === 'file') {
            const size = convertBytes(item.size)
            const duration = formatDuration(item.duration)
            const durationCell = (isVideoFile(item.name) || isAudioFile(item.name)) ? `<div class="duration-badge">${duration}</div>` : '-'
            
            html += `
                <div data-path="${item.path}" data-id="${item.id}" data-name="${item.name}" class="directory-item file-item">
                    <div class="item-name">
                        <div class="item-icon file">
                            ${getFileIcon(item.name)}
                        </div>
                        <div class="item-text">
                            <div class="item-title">${item.name}</div>
                        </div>
                    </div>
                    <div class="item-size">${size}</div>
                    <div class="item-duration">${durationCell}</div>
                    <div class="item-actions">
                        <button data-id="${item.id}" class="more-btn">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <circle cx="12" cy="12" r="1"/>
                                <circle cx="19" cy="12" r="1"/>
                                <circle cx="5" cy="12" r="1"/>
                            </svg>
                        </button>
                    </div>
                </div>
            `

            if (isTrash) {
                html += `
                    <div data-path="${item.path}" id="more-option-${item.id}" data-name="${item.name}" class="more-options">
                        <input class="more-options-focus" readonly="readonly" style="height:0;width:0;border:none;position:absolute">
                        <div id="restore-${item.id}" data-path="${item.path}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8"/>
                                <path d="M21 3v5h-5"/>
                                <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16"/>
                                <path d="M8 16H3v5"/>
                            </svg>
                            <span>Restore</span>
                        </div>
                        <div id="delete-${item.id}" data-path="${item.path}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polyline points="3,6 5,6 21,6"/>
                                <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                                <line x1="10" y1="11" x2="10" y2="17"/>
                                <line x1="14" y1="11" x2="14" y2="17"/>
                            </svg>
                            <span>Delete Permanently</span>
                        </div>
                    </div>
                `
            } else {
                html += `
                    <div data-path="${item.path}" id="more-option-${item.id}" data-name="${item.name}" class="more-options">
                        <input class="more-options-focus" readonly="readonly" style="height:0;width:0;border:none;position:absolute">
                        <div id="rename-${item.id}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>
                                <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>
                            </svg>
                            <span>Rename</span>
                        </div>
                        <div id="move-${item.id}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                                <polyline points="14,2 14,8 20,8"/>
                                <line x1="16" y1="13" x2="8" y2="13"/>
                                <line x1="16" y1="17" x2="8" y2="17"/>
                                <polyline points="10,9 9,9 8,9"/>
                            </svg>
                            <span>Move</span>
                        </div>
                        <div id="copy-${item.id}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
                                <path d="M5 15H4a2 2 0 0 1-2 2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
                            </svg>
                            <span>Copy</span>
                        </div>
                        <div id="trash-${item.id}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polyline points="3,6 5,6 21,6"/>
                                <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                            </svg>
                            <span>Archive</span>
                        </div>
                        <div id="share-${item.id}" class="more-options-item">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"/>
                                <polyline points="16,6 12,2 8,6"/>
                                <line x1="12" y1="2" x2="12" y2="15"/>
                            </svg>
                            <span>Share Material</span>
                        </div>
                        <div id="encode-${item.id}" class="more-options-item encode-option" style="display: none;">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <polygon points="23 7 16 12 23 17 23 7"/>
                                <rect x="1" y="5" width="15" height="14" rx="2" ry="2"/>
                            </svg>
                            <span>🎬 Encode Video</span>
                        </div>
                    </div>
                `
            }
        }
    }
    document.getElementById('directory-data').innerHTML = html

    if (!isTrash) {
        document.querySelectorAll('.folder-item').forEach(div => {
            div.ondblclick = openFolder;
        });
        document.querySelectorAll('.file-item').forEach(div => {
            div.ondblclick = openFile;
        });
    }

    document.querySelectorAll('.more-btn').forEach(div => {
        div.addEventListener('click', function (event) {
            event.preventDefault();
            event.stopPropagation();
            openMoreButton(div)
        });
    });

    // Update breadcrumb after showing directory
    if (window.updateBreadcrumb) {
        window.updateBreadcrumb();
    }

    // Update file count after showing directory
    if (window.updateFileCount) {
        window.updateFileCount({ contents: data });
    }
    
    // Check encoding support and show encode options for video files
    checkEncodingSupport();
    
    // Add event listeners for encode options
    document.querySelectorAll('.encode-option').forEach(option => {
        const id = option.id.split('-')[1];
        option.addEventListener('click', function() {
            const fileName = document.getElementById(`more-option-${id}`).getAttribute('data-name');
            const filePath = document.getElementById(`more-option-${id}`).getAttribute('data-path') + '/' + id;
            
            // Close the more options menu first
            const moreOptions = document.getElementById(`more-option-${id}`);
            const focusElement = moreOptions.querySelector('.more-options-focus');
            if (focusElement) {
                focusElement.blur();
            }
            
            // Show encoding modal
            showVideoEncodingModal(filePath, fileName);
        });
    });
}

// Helper function to check if file is a video
function isVideoFile(fileName) {
    const videoExtensions = ['.mp4', '.mkv', '.webm', '.mov', '.avi', '.ts', '.ogv', '.m4v', '.flv', '.wmv', '.3gp', '.mpg', '.mpeg'];
    const extension = fileName.toLowerCase().substring(fileName.lastIndexOf('.'));
    return videoExtensions.includes(extension);
}

function isAudioFile(fileName) {
    const audioExtensions = ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.opus', '.wma', '.aiff', '.alac'];
    const extension = fileName.toLowerCase().substring(fileName.lastIndexOf('.'));
    return audioExtensions.includes(extension);
}

function isImageFile(fileName) {
    const imageExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg', '.ico', '.avif', '.tiff', '.tif'];
    const extension = fileName.toLowerCase().substring(fileName.lastIndexOf('.'));
    return imageExtensions.includes(extension);
}

function getFileIcon(fileName) {
    if (isVideoFile(fileName)) return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2" ry="2"/></svg>`;
    if (isAudioFile(fileName)) return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/></svg>`;
    if (isImageFile(fileName)) return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/></svg>`;
    if (fileName.toLowerCase().endsWith('.pdf')) return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>`;
    return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14,2 14,8 20,8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10,9 9,9 8,9"/></svg>`;
}

// Helper function to format duration
function formatDuration(duration) {
    if (!duration || duration === 0) {
        return '';
    }
    
    const hours = Math.floor(duration / 3600);
    const minutes = Math.floor((duration % 3600) / 60);
    const seconds = Math.floor(duration % 60);
    
    if (hours > 0) {
        return `${hours}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
    } else {
        return `${minutes}:${seconds.toString().padStart(2, '0')}`;
    }
}

// Check if video encoding is supported and show encode options
async function checkEncodingSupport() {
    try {
        const data = { password: getPassword() };
        const response = await postJson('/api/checkVideoEncodingSupport', data);
        
        if (response.status === 'ok' && response.ffmpeg_available) {
            logger.info('Video encoding is supported, showing encode options');
            
            // Show encode options for video files
            document.querySelectorAll('.encode-option').forEach(option => {
                const id = option.id.split('-')[1];
                const moreOption = document.getElementById(`more-option-${id}`);
                if (!moreOption) return;
                
                const fileName = moreOption.getAttribute('data-name');
                if (!fileName) return;
                
                // Check if it's a video file
                const videoExtensions = ['.mp4', '.mkv', '.webm', '.mov', '.avi', '.ts', '.ogv', '.m4v', '.flv', '.wmv', '.3gp', '.mpg', '.mpeg'];
                const isVideo = videoExtensions.some(ext => fileName.toLowerCase().endsWith(ext));
                
                if (isVideo) {
                    option.style.display = 'flex';
                    console.log(`Showing encode option for video: ${fileName}`);
                }
            });
        } else {
            console.log('Video encoding not supported:', response);
        }
    } catch (error) {
        console.error('Encoding support check failed:', error);
    }
}

document.getElementById('search-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    const query = document.getElementById('file-search').value;
    console.log(query)
    if (query === '') {
        alert('Search field is empty');
        return;
    }
    const path = '/?path=/search_' + encodeURI(query);
    console.log(path)
    window.location = path;
});

// Loading Main Page
document.addEventListener('DOMContentLoaded', function () {
    const inputs = ['new-folder-name', 'rename-name', 'file-search']
    for (let i = 0; i < inputs.length; i++) {
        document.getElementById(inputs[i]).addEventListener('input', validateInput);
    }

    if (getCurrentPath().includes('/share_')) {
        getCurrentDirectoryWithSort()
    } else {
        if (getPassword() === null) {
            document.getElementById('bg-blur').style.zIndex = '2';
            document.getElementById('bg-blur').style.opacity = '0.5';

            document.getElementById('get-password').style.zIndex = '3';
            document.getElementById('get-password').style.opacity = '1';
        } else {
            getCurrentDirectoryWithSort()
        }
    }
});