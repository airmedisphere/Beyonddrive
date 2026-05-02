// Mobile menu functionality
document.addEventListener('DOMContentLoaded', function() {
    const mobileMenuToggle = document.getElementById('mobile-menu-toggle');
    const sidebar = document.getElementById('sidebar');
    const bgBlur = document.getElementById('bg-blur');
    
    // Toggle mobile menu
    mobileMenuToggle.addEventListener('click', function(e) {
        e.stopPropagation();
        toggleMobileMenu();
    });
    
    // Close menu when clicking outside
    document.addEventListener('click', function(e) {
        if (window.innerWidth <= 768) {
            const isClickInsideSidebar = sidebar.contains(e.target);
            const isClickOnToggle = mobileMenuToggle.contains(e.target);
            
            if (!isClickInsideSidebar && !isClickOnToggle && sidebar.classList.contains('open')) {
                closeMobileMenu();
            }
        }
    });
    
    // Close menu when clicking on sidebar links (mobile)
    const sidebarLinks = sidebar.querySelectorAll('a');
    sidebarLinks.forEach(link => {
        link.addEventListener('click', function() {
            if (window.innerWidth <= 768) {
                closeMobileMenu();
            }
        });
    });
    
    // Handle window resize
    window.addEventListener('resize', function() {
        if (window.innerWidth > 768) {
            closeMobileMenu();
        }
    });
    
    function toggleMobileMenu() {
        if (sidebar.classList.contains('open')) {
            closeMobileMenu();
        } else {
            openMobileMenu();
        }
    }
    
    function openMobileMenu() {
        sidebar.classList.add('open');
        sidebar.classList.add('mobile-open');
        bgBlur.style.zIndex = '150';
        bgBlur.style.opacity = '0.3';
    }
    
    function closeMobileMenu() {
        sidebar.classList.remove('open');
        sidebar.classList.remove('mobile-open');
        bgBlur.style.opacity = '0';
        setTimeout(() => {
            if (!bgBlur.style.opacity || bgBlur.style.opacity === '0') {
                bgBlur.style.zIndex = '-1';
            }
        }, 300);
    }
    
    // Prevent menu close when interacting with new-upload dropdown
    const newUpload = document.getElementById('new-upload');
    if (newUpload) {
        newUpload.addEventListener('click', function(e) {
            e.stopPropagation();
        });
    }
});

// Touch gesture support for mobile
let touchStartX = 0;
let touchStartY = 0;
let touchEndX = 0;
let touchEndY = 0;

document.addEventListener('touchstart', function(e) {
    touchStartX = e.changedTouches[0].screenX;
    touchStartY = e.changedTouches[0].screenY;
}, { passive: true });

document.addEventListener('touchend', function(e) {
    touchEndX = e.changedTouches[0].screenX;
    touchEndY = e.changedTouches[0].screenY;
    handleSwipeGesture();
}, { passive: true });

function handleSwipeGesture() {
    const sidebar = document.getElementById('sidebar');
    const swipeThreshold = 50;
    const swipeDistanceX = touchEndX - touchStartX;
    const swipeDistanceY = Math.abs(touchEndY - touchStartY);
    
    // Only handle horizontal swipes (ignore vertical scrolling)
    if (swipeDistanceY > swipeThreshold) return;
    
    // Swipe right to open menu (only if starting from left edge)
    if (swipeDistanceX > swipeThreshold && touchStartX < 50 && window.innerWidth <= 768) {
        if (!sidebar.classList.contains('open')) {
            document.getElementById('mobile-menu-toggle').click();
        }
    }
    
    // Swipe left to close menu
    if (swipeDistanceX < -swipeThreshold && sidebar.classList.contains('open')) {
        sidebar.classList.remove('open');
        const bgBlur = document.getElementById('bg-blur');
        bgBlur.style.opacity = '0';
        setTimeout(() => {
            if (!bgBlur.style.opacity || bgBlur.style.opacity === '0') {
                bgBlur.style.zIndex = '-1';
            }
        }, 300);
    }
}

// Improve touch interactions for file/folder items
document.addEventListener('DOMContentLoaded', function() {
    // Add touch feedback for interactive elements
    const interactiveElements = document.querySelectorAll('.body-tr, .more-btn, .sidebar-menu a, .new-button');
    
    interactiveElements.forEach(element => {
        element.addEventListener('touchstart', function() {
            this.style.transform = 'scale(0.98)';
        }, { passive: true });
        
        element.addEventListener('touchend', function() {
            this.style.transform = 'scale(1)';
        }, { passive: true });
        
        element.addEventListener('touchcancel', function() {
            this.style.transform = 'scale(1)';
        }, { passive: true });
    });
});

// Optimize modal positioning for mobile keyboards
function adjustModalForKeyboard() {
    const modals = document.querySelectorAll('.create-new-folder, .file-uploader');
    
    modals.forEach(modal => {
        const inputs = modal.querySelectorAll('input[type="text"], input[type="password"]');
        
        inputs.forEach(input => {
            input.addEventListener('focus', function() {
                if (window.innerWidth <= 768) {
                    setTimeout(() => {
                        modal.style.transform = 'translate(-50%, -60%)';
                    }, 300);
                }
            });
            
            input.addEventListener('blur', function() {
                if (window.innerWidth <= 768) {
                    modal.style.transform = 'translate(-50%, -50%)';
                }
            });
        });
    });
}

// Initialize keyboard adjustments
document.addEventListener('DOMContentLoaded', adjustModalForKeyboard);