const CACHE = 'tradingbot-v1';
const SHELL = [
    '/dashboard/',
    '/dashboard/static/manifest.json',
    '/dashboard/static/icon.svg',
];

self.addEventListener('install', e => {
    e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)));
    self.skipWaiting();
});

self.addEventListener('activate', e => {
    e.waitUntil(
        caches.keys().then(keys =>
            Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
        )
    );
    self.clients.claim();
});

self.addEventListener('fetch', e => {
    const url = new URL(e.request.url);
    // API calls always go to network — we always want fresh trading data
    const isApi = ['/dashboard/signals', '/dashboard/stats', '/dashboard/report', '/health']
        .some(p => url.pathname.startsWith(p));
    if (isApi) return;

    // Shell assets: cache-first, update cache in background
    e.respondWith(
        caches.match(e.request).then(cached => {
            const network = fetch(e.request).then(res => {
                if (res.ok) caches.open(CACHE).then(c => c.put(e.request, res.clone()));
                return res;
            });
            return cached || network;
        })
    );
});
