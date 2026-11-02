// Gold Trade service worker.
// Trading data must always be live, so nothing is served from cache:
// API calls and WebSocket go straight to the network. The worker only
// shows a clear offline page when the server/internet cannot be reached.
const OFFLINE_HTML = `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<meta name="theme-color" content="#FFFFFF">
<title>Gold Trade - Offline</title>
<style>
  body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;
       background:#F0F4F8;color:#0F172A;font-family:-apple-system,BlinkMacSystemFont,'Inter',sans-serif;padding:16px;box-sizing:border-box}
  .box{background:#fff;border:1px solid #E2E8F0;border-radius:14px;padding:24px;max-width:360px;width:100%;text-align:center;box-shadow:0 4px 16px rgba(0,0,0,.06)}
  h1{font-size:1.1rem;margin:0 0 8px}
  p{font-size:.9rem;color:#475569;margin:0 0 18px;line-height:1.45}
  button{min-height:44px;width:100%;border:0;border-radius:10px;background:#0F172A;color:#fff;font-size:.95rem;font-weight:700;cursor:pointer}
</style></head>
<body><div class="box">
  <h1>📡 Cannot reach the server</h1>
  <p>The internet or the server is down. Live rates and orders will work again once the connection is back.</p>
  <button onclick="location.reload()">Try again</button>
</div>
<script>window.addEventListener('online',()=>location.reload());</script>
</body></html>`;

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));

self.addEventListener('fetch', (event) => {
    if (event.request.mode !== 'navigate') return;
    event.respondWith(
        fetch(event.request).catch(() =>
            new Response(OFFLINE_HTML, { headers: { 'Content-Type': 'text/html; charset=utf-8' } })
        )
    );
});
