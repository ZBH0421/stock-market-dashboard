// ============================================
// Frontend Configuration
// ============================================

const API_BASE_URL = (window.location.hostname === "127.0.0.1" || window.location.hostname === "localhost" || window.location.protocol === "file:")
    ? "http://127.0.0.1:8000"
    : "https://stock-market-dashboard-q1eq.onrender.com"; // Auto-detect Backend 
