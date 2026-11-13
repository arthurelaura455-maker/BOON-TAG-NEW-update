'use strict';
// Servidor do Gorila Tag: serve o jogo (arquivos estáticos), relay de multiplayer por WebSocket (/ws)
// e proxy opcional de IA para o chat dos bots (/api/chat). Só depende de "ws".
const http=require('http'),fs=require('fs'),path=require('path');
const {WebSocketServer}=require('ws');
const PORT=+process.env.PORT||3000,ROOT=path.resolve(__dirname,'..');
const ALLOW=(process.env.ALLOWED_ORIGINS||'').split(',').map(s=>s.trim()).filter(Boolean);
const MIME={'.html':'text/html; charset=utf-8','.css':'text/css; charset=utf-8','.js':'text/javascript; charset=utf-8','.json':'application/json','.webmanifest':'application/manifest+json','.svg':'image/svg+xml','.gz':'application/octet-stream','.glb':'model/gltf-binary'};
const PUB=/^\/(index\.html|css\/|js\/|assets\/|vendor\/|manifest\.webmanifest|favicon\.svg)/;
const hits=new Map();
function limited(k,max,win){const n=Date.now(),a=(hits.get(k)||[]).filter(t=>n-t<win);a.push(n);hits.set(k,a);return a.length>max}
setInterval(()=>{const n=Date.now();hits.forEach((a,k)=>{if(!a.some(t=>n-t<60000))hits.delete(k)})},60000).unref();
const ipOf=req=>(process.env.TRUST_PROXY&&String(req.headers['x-forwarded-for']||'').split(',')[0].trim())||req.socket.remoteAddress;

async function chat(req,res){
  if(limited('c'+ipOf(req),10,60000)){res.writeHead(429);return res.end('{}')}
  const key=process.env.ANTHROPIC_API_KEY;if(!key){res.writeHead(503);return res.end('{}')}
  let b='';for await(const c of req){b+=c;if(b.length>8192){res.writeHead(413);return res.end('{}')}}
  try{
    const {prompt}=JSON.parse(b);if(typeof prompt!=='string'||!prompt||prompt.length>3000)throw new Error('bad');
    const r=await fetch('https://api.anthropic.com/v1/messages',{method:'POST',headers:{'x-api-key':key,'anthropic-version':'2023-06-01','content-type':'application/json'},
      body:JSON.stringify({model:process.env.CHAT_MODEL||'claude-haiku-5-5',max_tokens:150,messages:[{role:'user',content:prompt}]})});
    const j=await r.json();const text=(j.content&&j.content[0]&&j.content[0].text)||'';
    res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify({text:text.slice(0,400)}));
  }catch(e){res.writeHead(502);res.end('{}')}
}

const server=http.createServer((req,res)=>{
  try{
    const url=new URL(req.url,'http://x'),o=req.headers.origin;
    if(o&&(!ALLOW.length||ALLOW.includes(o))){res.setHeader('Access-Control-Allow-Origin',o);res.setHeader('Access-Control-Allow-Headers','Content-Type');res.setHeader('Vary','Origin')}
    if(req.method==='OPTIONS'){res.writeHead(204);return res.end()}
    if(url.pathname==='/api/chat'&&req.method==='POST')return chat(req,res);
    if(url.pathname==='/healthz'){res.writeHead(200);return res.end('ok')}
    if(req.method!=='GET'&&req.method!=='HEAD'){res.writeHead(405);return res.end()}
    let p=decodeURIComponent(url.pathname);if(p==='/')p='/index.html';
    if(!PUB.test(p)){res.writeHead(404);return res.end('Not found')}
    const f=path.join(ROOT,path.normalize(p));if(!f.startsWith(ROOT+path.sep)){res.writeHead(403);return res.end()}
    fs.stat(f,(e,st)=>{
      if(e||!st.isFile()){res.writeHead(404);return res.end('Not found')}
      res.writeHead(200,{'Content-Type':MIME[path.extname(f)]||'application/octet-stream','Content-Length':st.size,'Cache-Control':p.startsWith('/assets/')?'public, max-age=86400':'no-cache'});
      if(req.method==='HEAD')return res.end();fs.createReadStream(f).pipe(res);
    });
  }catch(e){res.writeHead(400);res.end()}
});

// ---- multiplayer: salas com presença por jogador e eventos (chat, sinalização de voz)
const NAME=/^[a-z0-9][a-z0-9_.-]{0,47}$/,TOPIC=/^[a-z][a-z0-9_.-]{0,47}$/;
const rooms=new Map();let nid=0;
const wss=new WebSocketServer({server,path:'/ws',maxPayload:16384});
const send=(ws,o)=>{if(ws.readyState===1)ws.send(typeof o==='string'?o:JSON.stringify(o))};
const cap=n=>n==='lobby'?200:16;
function snapshot(name,r){return JSON.stringify({t:'peers',room:name,peers:[...r.peers].map(([peer,e])=>({peer,presence:e.pres}))})}
function leave(ws,name){const r=rooms.get(name);if(!r)return;r.peers.delete(ws.id);ws.rooms.delete(name);if(!r.peers.size)rooms.delete(name);else r.dirty=true}
wss.on('connection',(ws,req)=>{
  if(ALLOW.length&&!ALLOW.includes(req.headers.origin||'')){ws.close(1008);return}
  ws.id=(++nid).toString(36)+Math.random().toString(36).slice(2,6);ws.rooms=new Set();ws.tok=120;ws.ts=Date.now();ws.alive=true;
  ws.on('pong',()=>{ws.alive=true});
  send(ws,{t:'hello',id:ws.id});
  ws.on('message',raw=>{
    const n=Date.now();ws.tok=Math.min(120,ws.tok+(n-ws.ts)/1000*60);ws.ts=n;if(--ws.tok<0)return;
    let m;try{m=JSON.parse(raw)}catch(e){return}
    if(!m||typeof m.t!=='string'||typeof m.room!=='string')return;const name=m.room;
    if(m.t==='join'){
      if(!NAME.test(name)){return send(ws,{t:'err',room:name,code:'invalid_argument'})}
      let r=rooms.get(name);
      if(!(r&&r.peers.has(ws.id))){
        if(ws.rooms.size>=16)return send(ws,{t:'err',room:name,code:'limit_reached'});
        if(r&&r.peers.size>=cap(name))return send(ws,{t:'err',room:name,code:'limit_reached'});
        if(!r)rooms.set(name,r={peers:new Map(),dirty:false});
        r.peers.set(ws.id,{ws,pres:{}});ws.rooms.add(name);
      }
      r.dirty=true;send(ws,snapshot(name,r));
    }else{
      const r=rooms.get(name);if(!r||!r.peers.has(ws.id))return;
      if(m.t==='pres'){if(!m.p||typeof m.p!=='object'||Array.isArray(m.p)||JSON.stringify(m.p).length>4096)return;r.peers.get(ws.id).pres=m.p;r.dirty=true}
      else if(m.t==='emit'){
        if(!TOPIC.test(m.topic||'')||JSON.stringify(m.data===undefined?null:m.data).length>4096)return;
        const out=JSON.stringify({t:'msg',room:name,topic:m.topic,data:m.data,peer:ws.id});r.peers.forEach(e=>send(e.ws,out));
      }else if(m.t==='leave')leave(ws,name);
    }
  });
  ws.on('close',()=>{[...ws.rooms].forEach(nm=>leave(ws,nm))});
  ws.on('error',()=>{});
});
setInterval(()=>{rooms.forEach((r,name)=>{if(!r.dirty)return;r.dirty=false;const s=snapshot(name,r);r.peers.forEach(e=>send(e.ws,s))})},66);
setInterval(()=>{wss.clients.forEach(ws=>{if(!ws.alive)return ws.terminate();ws.alive=false;ws.ping()})},30000);
server.listen(PORT,()=>console.log('Gorila Tag em http://localhost:'+PORT));
