/**
 * STATXTRACT USER PORTAL — MOBILE RESPONSIVENESS CONTROLLER
 * Manages responsive sidebar overlay, hamburger buttons, and layout fixes on mobile.
 */

document.addEventListener('DOMContentLoaded', () => {
  initMobileSidebar();
});

function initMobileSidebar() {
  const sb = document.getElementById('sidebar');
  const toggleBtn = document.getElementById('sidebarToggle') || document.querySelector('.sidebar-toggle');
  if (!sb || !toggleBtn) return;

  // 1. Create mobile backdrop overlay dynamically if it doesn't exist
  let overlay = document.getElementById('sidebarOverlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.className = 'sidebar-overlay';
    overlay.id = 'sidebarOverlay';
    // Insert overlay right after the sidebar
    sb.parentNode.insertBefore(overlay, sb.nextSibling);
  }

  // 2. Handle click on toggleBtn (Hamburger menu on mobile)
  toggleBtn.addEventListener('click', (e) => {
    if (window.innerWidth <= 768) {
      e.stopPropagation(); // Prevent immediate close from document click listener
      sb.classList.toggle('mobile-open');
      overlay.classList.toggle('active');
    }
  });

  // 3. Handle click on overlay to close the sidebar
  overlay.addEventListener('click', () => {
    sb.classList.remove('mobile-open');
    overlay.classList.remove('active');
  });

  // 4. Handle click outside the sidebar to close it
  document.addEventListener('click', (e) => {
    if (window.innerWidth <= 768) {
      const isClickInsideSidebar = sb.contains(e.target);
      const isClickOnToggle = toggleBtn === e.target || toggleBtn.contains(e.target);
      
      if (!isClickInsideSidebar && !isClickOnToggle && sb.classList.contains('mobile-open')) {
        sb.classList.remove('mobile-open');
        overlay.classList.remove('active');
      }
    }
  });

  // 5. Close sidebar on navigation item clicks
  const navItems = sb.querySelectorAll('.nav-item');
  navItems.forEach(item => {
    item.addEventListener('click', () => {
      if (window.innerWidth <= 768) {
        sb.classList.remove('mobile-open');
        overlay.classList.remove('active');
      }
    });
  });
}
