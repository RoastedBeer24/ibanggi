/* 이야기방 서비스 워커: 항상 최신 파일을 먼저 받고, 오프라인일 때만 저장본을 씀 */
const CACHE = 'ibanggi-v13';
const FILES = ['./', './index.html', './manifest.webmanifest', './icon-192.png', './icon-512.png'];
self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(FILES)));
  self.skipWaiting();
});
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const u = new URL(e.request.url);
  if (e.request.method !== 'GET' || u.origin !== location.origin) return;
  e.respondWith(
    fetch(e.request).then(r => {
      if (r.ok) { const copy = r.clone(); caches.open(CACHE).then(c => c.put(e.request, copy)); }
      return r;
    }).catch(() => caches.match(e.request).then(r => r || caches.match('./index.html')))
  );
});

/* ===== 백그라운드 생성: 페이지가 멈추거나 닫혀도 서비스 워커가 응답을 끝까지 받아 저장하고 알림 ===== */
const JDB = 'ibanggi-jobs';
function jdb(){
  return new Promise((res, rej) => {
    const r = indexedDB.open(JDB, 1);
    r.onupgradeneeded = () => r.result.createObjectStore('jobs', {keyPath:'id'});
    r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error);
  });
}
async function jtx(fn){
  const db = await jdb();
  try{ await new Promise((res, rej) => { const tx = db.transaction('jobs', 'readwrite'); fn(tx.objectStore('jobs')); tx.oncomplete = res; tx.onerror = () => rej(tx.error); tx.onabort = () => rej(tx.error); }); }
  finally{ db.close(); }
}
const jput = rec => jtx(st => st.put(rec)).catch(() => {});
const jdel = id => jtx(st => st.delete(id)).catch(() => {});
const running = new Map();

function extract(kind, j, rec){
  if(kind === 'gemini'){
    const c = j.candidates && j.candidates[0];
    if(!c || !c.content || !c.content.parts) return '';
    return c.content.parts.map(p => p.thought ? '' : (p.text || '')).join('');
  }
  if(kind === 'claude'){
    if(j.type === 'message_start' && j.message && j.message.usage) Object.assign(rec.usage, j.message.usage);
    if(j.type === 'message_delta' && j.usage) Object.assign(rec.usage, j.usage);
    return j.type === 'content_block_delta' && j.delta && j.delta.type === 'text_delta' ? (j.delta.text || '') : '';
  }
  const d = j.choices && j.choices[0] && j.choices[0].delta;
  return (d && d.content) || '';
}
async function readSSE(r, onData){
  const reader = r.body.getReader(), dec = new TextDecoder();
  let buf = '';
  const handle = block => {
    const data = block.split(/\r?\n/).filter(l => l.startsWith('data:')).map(l => l.slice(5).replace(/^ /, '')).join('\n');
    if(!data || data === '[DONE]') return;
    let j; try{ j = JSON.parse(data); }catch(e){ return; }
    onData(j);
  };
  try{
    for(;;){
      const {done, value} = await reader.read();
      if(done) break;
      buf += dec.decode(value, {stream:true});
      let i;
      while((i = buf.search(/\r?\n\r?\n/)) >= 0){ handle(buf.slice(0, i)); buf = buf.slice(i).replace(/^\r?\n\r?\n/, ''); }
    }
    if(buf.trim()) handle(buf);
  } finally { try{ reader.cancel(); }catch(e){} }
}
async function windows(){ return self.clients.matchAll({type:'window', includeUncontrolled:true}); }
function preview(t){
  return String(t || '').replace(/\[\[[\s\S]*?\]\]/g, '').replace(/\[\[[\s\S]*$/, '').replace(/[*_#>`]/g, '').replace(/\s+/g, ' ').trim().slice(0, 90);
}
async function runJob(job, src){
  const ac = new AbortController(); running.set(job.id, ac);
  const rec = {id:job.id, meta:job.meta || {}, status:'running', text:'', usage:{}, t0:Date.now()};
  await jput(rec);
  const send = m => { try{ src && src.postMessage(m); }catch(e){} };
  let saved = Date.now();
  try{
    const r = await fetch(job.url, {method:'POST', headers:job.headers, body:job.body, signal:ac.signal});
    if(!r.ok){ rec.status = 'httperr'; rec.http = {status:r.status, body:await r.text().catch(() => '')}; }
    else {
      await readSSE(r, j => {
        const t = extract(job.kind, j, rec); if(t) rec.text += t;
        send({type:'job-data', id:job.id, j});
        if(Date.now() - saved > 2000){ saved = Date.now(); jput(Object.assign({}, rec)); }
      });
      rec.status = 'done';
    }
  }catch(e){
    rec.status = e && e.name === 'AbortError' ? 'aborted' : 'error';
    rec.error = String((e && e.message) || e); rec.net = e instanceof TypeError;
  }
  running.delete(job.id);
  rec.t1 = Date.now();
  if(rec.status === 'aborted') await jdel(job.id); else await jput(rec);
  const end = {type:'job-end', id:job.id, status:rec.status, text:rec.text, usage:rec.usage, http:rec.http, error:rec.error, net:rec.net};
  const ws = await windows();
  ws.forEach(c => { try{ c.postMessage(end); }catch(e){} });
  const n = job.notify;
  if(n && rec.status !== 'aborted' && !ws.some(c => c.visibilityState === 'visible')){
    const ok = rec.status === 'done';
    const body = ok ? (n.preview && preview(rec.text)) || n.body || '답변이 도착했어요' : (n.failBody || '받지 못했어요. 열어서 다시 시도해 주세요');
    try{ await self.registration.showNotification(n.title || '이야기방', {body, tag:'ibanggi-' + (n.sid || job.id), renotify:true, icon:'icon-192.png', badge:'icon-192.png', data:{sid:n.sid || '', kind:(job.meta || {}).kind || ''}}); }catch(e){}
  }
}
self.addEventListener('message', e => {
  const d = e.data || {};
  if(d.type === 'job') e.waitUntil(runJob(d.job, e.source));
  else if(d.type === 'job-abort'){ const a = running.get(d.id); if(a) a.abort(); }
  else if(d.type === 'job-ack') e.waitUntil(jdel(d.id));
  else if(d.type === 'job-list' && e.ports[0]) e.ports[0].postMessage({ids:[...running.keys()]});
});
self.addEventListener('notificationclick', e => {
  e.notification.close();
  const data = e.notification.data || {};
  e.waitUntil((async () => {
    const ws = await windows();
    for(const c of ws){
      try{ await c.focus(); c.postMessage({type:'open-session', sid:data.sid, kind:data.kind}); return; }catch(err){}
    }
    await self.clients.openWindow('./' + (data.sid ? '?s=' + encodeURIComponent(data.sid) : ''));
  })());
});
