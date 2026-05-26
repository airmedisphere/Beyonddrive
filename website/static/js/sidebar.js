// Handling New Button On Sidebar Click
const isTrash = getCurrentPath().startsWith('/trash')
const isSearch = getCurrentPath().startsWith('/search')
const isShare = getCurrentPath().startsWith('/share')

if (!isTrash && !isSearch) {
    document.getElementById('new-button').addEventListener('click', () => {
        document.getElementById('new-upload').style.zIndex = '1'
        document.getElementById('new-upload').style.opacity = '1'
        document.getElementById('new-upload').style.top = '80px'
        document.getElementById('new-upload-focus').focus()
    });
}
else {
    document.getElementById('new-button').style.display = 'none'
}

if (isShare) {
    document.getElementById('new-button').style.display = 'none'
    const sections = document.querySelector('.sidebar-menu').getElementsByTagName('a')
    sections[1].remove()
}

// New File Upload Start

function closeNewUploadFocus() {
    setTimeout(() => {
        document.getElementById('new-upload').style.opacity = '0'
        document.getElementById('new-upload').style.top = '40px'
        setTimeout(() => {
            document.getElementById('new-upload').style.zIndex = '-1'
        }, 300)
    }, 200)
}
document.getElementById('new-upload-focus').addEventListener('blur', closeNewUploadFocus);
document.getElementById('new-upload-focus').addEventListener('focusout', closeNewUploadFocus);

document.getElementById('file-upload-btn').addEventListener('click', () => {
    document.getElementById('fileInput').click()
});

// New File Upload End

// New Folder Start

document.getElementById('new-folder-btn').addEventListener('click', () => {
    document.getElementById('new-folder-name').value = '';
    document.getElementById('bg-blur').style.zIndex = '2';
    document.getElementById('bg-blur').style.opacity = '0.1';

    document.getElementById('create-new-folder').style.zIndex = '3';
    document.getElementById('create-new-folder').style.opacity = '1';
    setTimeout(() => {
        document.getElementById('new-folder-name').focus();
    }, 300)
})

document.getElementById('new-folder-cancel').addEventListener('click', () => {
    document.getElementById('new-folder-name').value = '';
    document.getElementById('bg-blur').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('bg-blur').style.zIndex = '-1';
    }, 300)
    document.getElementById('create-new-folder').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('create-new-folder').style.zIndex = '-1';
    }, 300)
});

document.getElementById('new-folder-create').addEventListener('click', createNewFolder);

// New Folder End

// New Url Upload Start

document.getElementById('url-upload-btn').addEventListener('click', () => {
    document.getElementById('remote-url').value = '';
    document.getElementById('bg-blur').style.zIndex = '2';
    document.getElementById('bg-blur').style.opacity = '0.1';

    document.getElementById('new-url-upload').style.zIndex = '3';
    document.getElementById('new-url-upload').style.opacity = '1';
    setTimeout(() => {
        document.getElementById('remote-url').focus();
    }, 300)
})

document.getElementById('remote-cancel').addEventListener('click', () => {
    document.getElementById('remote-url').value = '';
    document.getElementById('bg-blur').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('bg-blur').style.zIndex = '-1';
    }, 300)
    document.getElementById('new-url-upload').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('new-url-upload').style.zIndex = '-1';
    }, 300)
});

document.getElementById('remote-start').addEventListener('click', Start_URL_Upload);

// New Url Upload End

// Smart Bulk Import Start

document.getElementById('smart-bulk-import-btn').addEventListener('click', () => {
    document.getElementById('smart-bulk-channel').value = '';
    document.getElementById('smart-bulk-start-msg').value = '';
    document.getElementById('smart-bulk-end-msg').value = '';
    document.querySelector('input[name="import-mode"][value="auto"]').checked = true;
    document.getElementById('channel-status').style.display = 'none';
    
    document.getElementById('bg-blur').style.zIndex = '2';
    document.getElementById('bg-blur').style.opacity = '0.1';

    document.getElementById('smart-bulk-import-modal').style.zIndex = '3';
    document.getElementById('smart-bulk-import-modal').style.opacity = '1';
    setTimeout(() => {
        document.getElementById('smart-bulk-channel').focus();
    }, 300)
})

document.getElementById('smart-bulk-cancel').addEventListener('click', () => {
    document.getElementById('smart-bulk-channel').value = '';
    document.getElementById('smart-bulk-start-msg').value = '';
    document.getElementById('smart-bulk-end-msg').value = '';
    document.getElementById('channel-status').style.display = 'none';
    
    document.getElementById('bg-blur').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('bg-blur').style.zIndex = '-1';
    }, 300)
    document.getElementById('smart-bulk-import-modal').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('smart-bulk-import-modal').style.zIndex = '-1';
    }, 300)
});

document.getElementById('smart-bulk-check').addEventListener('click', checkChannel);
document.getElementById('smart-bulk-start').addEventListener('click', Start_Smart_Bulk_Import);

// Smart Bulk Import End

// Restricted Content Import Start

document.getElementById('restricted-import-btn').addEventListener('click', () => {
    document.getElementById('restricted-links').value = '';

    document.getElementById('bg-blur').style.zIndex = '2';
    document.getElementById('bg-blur').style.opacity = '0.1';

    document.getElementById('restricted-import-modal').style.zIndex = '3';
    document.getElementById('restricted-import-modal').style.opacity = '1';
    setTimeout(() => {
        document.getElementById('restricted-links').focus();
    }, 300);
});

document.getElementById('restricted-cancel').addEventListener('click', () => {
    document.getElementById('restricted-links').value = '';

    document.getElementById('bg-blur').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('bg-blur').style.zIndex = '-1';
    }, 300);
    document.getElementById('restricted-import-modal').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('restricted-import-modal').style.zIndex = '-1';
    }, 300);
});

document.getElementById('restricted-start').addEventListener('click', Start_Restricted_Import);

// Restricted Content Import End


// Bulk Delete Start

document.getElementById('bulk-delete-btn').addEventListener('click', () => {
    document.getElementById('bulk-delete-start').value = '';
    document.getElementById('bulk-delete-end').value = '';
    document.getElementById('bulk-delete-preview').style.display = 'none';
    document.getElementById('bulk-delete-preview').innerHTML = '';
    document.getElementById('bulk-delete-confirm-btn').style.display = 'none';

    document.getElementById('bg-blur').style.zIndex = '2';
    document.getElementById('bg-blur').style.opacity = '0.1';

    document.getElementById('bulk-delete-modal').style.zIndex = '3';
    document.getElementById('bulk-delete-modal').style.opacity = '1';
    setTimeout(() => {
        document.getElementById('bulk-delete-start').focus();
    }, 300);
});

document.getElementById('bulk-delete-cancel-btn').addEventListener('click', () => {
    document.getElementById('bulk-delete-start').value = '';
    document.getElementById('bulk-delete-end').value = '';
    document.getElementById('bulk-delete-preview').style.display = 'none';

    document.getElementById('bg-blur').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('bg-blur').style.zIndex = '-1';
    }, 300);
    document.getElementById('bulk-delete-modal').style.opacity = '0';
    setTimeout(() => {
        document.getElementById('bulk-delete-modal').style.zIndex = '-1';
    }, 300);
});

document.getElementById('bulk-delete-preview-btn').addEventListener('click', Bulk_Delete_Preview);
document.getElementById('bulk-delete-confirm-btn').addEventListener('click', Bulk_Delete_Confirm);

// Bulk Delete End
