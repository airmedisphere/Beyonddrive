// File count functionality
let fileCountData = {
    videos: 0,
    pdfs: 0,
    images: 0,
    documents: 0,
    audio: 0,
    folders: 0,
    others: 0,
    total: 0
};

// File type definitions
const fileTypes = {
    video: ['.mp4', '.mkv', '.webm', '.mov', '.avi', '.ts', '.ogv', '.m4v', '.flv', '.wmv', '.3gp', '.mpg', '.mpeg'],
    pdf: ['.pdf'],
    image: ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp', '.ico', '.tiff', '.tif'],
    document: ['.doc', '.docx', '.txt', '.rtf', '.odt', '.pages', '.tex', '.wpd'],
    audio: ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a', '.opus', '.aiff']
};

// Get file type category
function getFileTypeCategory(fileName) {
    const extension = fileName.toLowerCase().substring(fileName.lastIndexOf('.'));
    
    for (const [category, extensions] of Object.entries(fileTypes)) {
        if (extensions.includes(extension)) {
            return category;
        }
    }
    
    return 'others';
}

// Count files by type
function countFilesByType(directoryData) {
    const counts = {
        videos: 0,
        pdfs: 0,
        images: 0,
        documents: 0,
        audio: 0,
        folders: 0,
        others: 0,
        total: 0
    };

    const contents = directoryData.contents || {};
    
    for (const [key, item] of Object.entries(contents)) {
        if (item.type === 'folder') {
            counts.folders++;
        } else if (item.type === 'file') {
            const category = getFileTypeCategory(item.name);
            counts[category + 's']++; // Convert category to plural (video -> videos)
        }
        counts.total++;
    }

    return counts;
}

// Update file count display
function updateFileCountDisplay(counts) {
    fileCountData = counts;
    const statsContainer = document.getElementById('file-count-stats');
    
    if (!statsContainer) return;

    if (counts.total === 0) {
        statsContainer.style.display = 'none';
        return;
    }

    // Use grid display (matches new CSS grid-template-columns: repeat(3,1fr))
    statsContainer.style.display = 'grid';

    statsContainer.innerHTML = '';

    const countItems = [
        {
            key: 'total',
            label: 'Total Items',
            iconClass: 'total',
            svgPath: `<path d="M19,3H5C3.89,3 3,3.89 3,5V19A2,2 0 0,0 5,21H19A2,2 0 0,0 21,19V5C21,3.89 20.1,3 19,3M19,5V19H5V5H19Z"/>`
        },
        {
            key: 'videos',
            label: 'Video Lessons',
            iconClass: 'video',
            svgPath: `<polygon points="23 7 16 12 23 17 23 7"/><rect x="1" y="5" width="15" height="14" rx="2" ry="2"/>`
        },
        {
            key: 'pdfs',
            label: 'PDF Materials',
            iconClass: 'pdf',
            svgPath: `<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14,2 14,8 20,8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/>`
        },
        {
            key: 'folders',
            label: 'Course Modules',
            iconClass: 'total',
            svgPath: `<path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>`
        },
        {
            key: 'images',
            label: 'Images',
            iconClass: 'total',
            svgPath: `<rect x="3" y="3" width="18" height="18" rx="2" ry="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21,15 16,10 5,21"/>`
        },
        {
            key: 'documents',
            label: 'Documents',
            iconClass: 'pdf',
            svgPath: `<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14,2 14,8 20,8"/>`
        },
        {
            key: 'audio',
            label: 'Audio Files',
            iconClass: 'total',
            svgPath: `<path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/>`
        },
        {
            key: 'others',
            label: 'Other Files',
            iconClass: 'total',
            svgPath: `<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14,2 14,8 20,8"/>`
        }
    ];

    countItems.forEach(item => {
        const count = counts[item.key];
        if (count > 0) {
            const statsItem = document.createElement('div');
            statsItem.className = 'stats-item';
            statsItem.innerHTML = `
                <div class="stats-icon ${item.iconClass}">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">${item.svgPath}</svg>
                </div>
                <div class="stats-text">
                    <div class="stats-number">${count}</div>
                    <div class="stats-label">${item.label}</div>
                </div>
            `;
            statsContainer.appendChild(statsItem);
        }
    });
}

// Hide file count stats for special paths
function shouldShowFileCount(currentPath) {
    return !currentPath.startsWith('/trash') && 
           !currentPath.startsWith('/search_') && 
           !currentPath.startsWith('/share_');
}

// Update file count when directory changes
function updateFileCount(directoryData) {
    const currentPath = getCurrentPath();
    
    if (shouldShowFileCount(currentPath)) {
        const counts = countFilesByType(directoryData);
        updateFileCountDisplay(counts);
    } else {
        // Hide file count stats for special paths
        const statsContainer = document.getElementById('file-count-stats');
        if (statsContainer) {
            statsContainer.style.display = 'none';
        }
    }
}

// Export function for use in main.js
window.updateFileCount = updateFileCount;

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    // File count will be updated when directory is loaded
});