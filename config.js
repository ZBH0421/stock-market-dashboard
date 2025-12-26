// ============================================
// Frontend Configuration
// ============================================

// Automatically detect if running on localhost or production
const API_BASE_URL = (window.location.hostname === "127.0.0.1" || window.location.hostname === "localhost")
    ? "http://127.0.0.1:8000"
    : "https://stock-market-dashboard-qieq.onrender.com"; // Render Backend URL 
