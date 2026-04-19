/* ═══════════════════════════════════════════
   CcMart BI Dashboard — app.js
   Real-time simulation + all charts + interactions
═══════════════════════════════════════════ */

// ── Chart.js defaults ────────────────────
Chart.defaults.color = '#9ea3c8';
Chart.defaults.borderColor = 'rgba(255,255,255,0.06)';
Chart.defaults.font.family = "'Inter',sans-serif";
Chart.defaults.plugins.legend.labels.boxWidth = 10;
Chart.defaults.plugins.legend.labels.padding = 14;

const P = ['#6366f1','#10b981','#06b6d4','#f59e0b','#ec4899','#8b5cf6','#ef4444','#14b8a6'];

// ═══════════════════════════════════════════
// NAVIGATION
// ═══════════════════════════════════════════
const pages = {
  ops:'Live Operations', revenue:'Revenue & Sales',
  customers:'Customer Intelligence', conversion:'Conversion Center',
  ml:'ML Predictions', products:'Product Analytics',
  alerts:'Alerts & Actions', pipeline:'Data Pipeline'
};
const initDone = new Set();

document.querySelectorAll('.sb-item').forEach(el=>{
  el.addEventListener('click', e=>{
    e.preventDefault();
    const pg = el.dataset.page;
    switchPage(pg, el);
  });
});

function switchPage(pg, el){
  document.querySelectorAll('.sb-item').forEach(i=>i.classList.remove('active'));
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  const item = el || document.querySelector(`.sb-item[data-page="${pg}"]`);
  if(item) item.classList.add('active');
  const target = document.getElementById('page-'+pg);
  if(target) target.classList.add('active');
  document.getElementById('crumb').textContent = pages[pg]||pg;
  if(window.innerWidth<=800) document.getElementById('sidebar').classList.remove('open');
  if(!initDone.has(pg)){ initDone.add(pg); lazyInit(pg); }
}

// ═══════════════════════════════════════════
// LIVE DATA STATE
// ═══════════════════════════════════════════
let revenue = 0, orders = 0, successOrders = 0, abandoned = 0, sessions = 1247;
let atRisk = 0, feedPaused = false;
const revenueHistory = Array(30).fill(0);
const sparkHistory = { orders:Array(12).fill(0), success:Array(12).fill(0), abandon:Array(12).fill(0), sessions:Array(12).fill(0) };

// ═══════════════════════════════════════════
// TOPBAR DATE
// ═══════════════════════════════════════════
function updateDate(){
  const now = new Date();
  document.getElementById('tbDate').textContent =
    now.toLocaleDateString('en-ID',{weekday:'short',day:'numeric',month:'short',year:'numeric'}) +
    ' · ' + now.toLocaleTimeString('en-ID',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
}
setInterval(updateDate, 1000); updateDate();

// ═══════════════════════════════════════════
// LAST SYNC
// ═══════════════════════════════════════════
setInterval(()=>{
  const el = document.getElementById('lastSync');
  if(el) el.textContent = new Date().toLocaleTimeString('en-ID',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
},1000);

// ═══════════════════════════════════════════
// HELPER: make chart
// ═══════════════════════════════════════════
function mkChart(id, cfg){
  const c = document.getElementById(id);
  if(!c) return null;
  if(c._ch){ c._ch.destroy(); }
  const ch = new Chart(c, cfg);
  c._ch = ch; return ch;
}

// ═══════════════════════════════════════════
// SPARKLINE
// ═══════════════════════════════════════════
function drawSpark(id, data, color){
  const c = document.getElementById(id);
  if(!c) return;
  if(c._ch) c._ch.destroy();
  c._ch = new Chart(c,{
    type:'line',
    data:{ labels:data.map((_,i)=>i), datasets:[{data,borderColor:color,borderWidth:1.5,pointRadius:0,fill:true,backgroundColor:color+'22',tension:.4}] },
    options:{ animation:false,plugins:{legend:{display:false},tooltip:{enabled:false}},scales:{x:{display:false},y:{display:false}} }
  });
}

// ═══════════════════════════════════════════
// LIVE ORDER FEED
// ═══════════════════════════════════════════
const NAMES = ['Siti S.','Eva W.','Budi P.','Rina A.','Joko S.','Dewi R.','Ahmad F.','Maya L.','Rizki H.','Nadia K.','Fajar M.','Lestari D.'];
const PRODS = ['Nike Air Max 270','H&M Floral Dress','GoPro Hero 12','Adidas Polo Shirt','IKEA Desk Lamp','Puma Sports Bag','Zara Mini Bag','OnePlus Buds Pro','Ray-Ban Sunglasses','Under Armour Shorts'];
const PAYS  = ['Credit Card','GoPay','OVO','Debit Card','LinkAja'];
const CATS  = ['Footwear','Apparel','Electronics','Accessories','Home'];

function randomOrder(){
  const rnd = Math.random();
  const status = rnd > 0.043 ? 'success' : (rnd < 0.02 ? 'fail' : 'pending');
  const amount = Math.floor(Math.random()*800+200)*1000;
  const name = NAMES[Math.floor(Math.random()*NAMES.length)];
  const prod = PRODS[Math.floor(Math.random()*PRODS.length)];
  const pay  = PAYS[Math.floor(Math.random()*PAYS.length)];
  return {status, amount, name, prod, pay};
}

function addOrderToFeed(o){
  if(feedPaused) return;
  const feed = document.getElementById('orderFeed');
  if(!feed) return;
  const empty = feed.querySelector('.feed-empty');
  if(empty) empty.remove();
  const icons = {success:'✅', fail:'❌', pending:'⏳'};
  const now = new Date().toLocaleTimeString('en-ID',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
  const div = document.createElement('div');
  div.className = `order-item oi-${o.status}`;
  div.innerHTML = `
    <span class="oi-icon">${icons[o.status]}</span>
    <div class="oi-info">
      <div class="oi-cust">${o.name} · ${o.pay}</div>
      <div class="oi-prod">${o.prod}</div>
    </div>
    <div class="oi-amt">₹${(o.amount).toLocaleString()}</div>
    <div class="oi-time">${now}</div>`;
  feed.insertBefore(div, feed.firstChild);
  while(feed.children.length > 40) feed.removeChild(feed.lastChild);
}

window.toggleFeedPause = function(){
  feedPaused = !feedPaused;
  const btn = document.getElementById('feedPauseBtn');
  if(btn) btn.textContent = feedPaused ? '▶ Resume' : '⏸ Pause';
};

// ═══════════════════════════════════════════
// SESSION AT-RISK LIST
// ═══════════════════════════════════════════
const riskSessions = [];
function refreshRiskSessions(){
  const el = document.getElementById('sessionList');
  if(!el || !el.offsetParent) return;
  // Add new
  if(Math.random()<0.4){
    const prob = Math.random();
    const label = prob >= 0.7 ? 'HIGH' : prob >= 0.4 ? 'MEDIUM' : null;
    if(label){
      riskSessions.unshift({
        id:'sess_'+Math.random().toString(36).slice(2,8),
        prob: Math.round(prob*100),
        label,
        events: Math.floor(Math.random()*20)+5,
        src: Math.random()>0.1?'Mobile':'Web',
        cart: Math.random()>0.5
      });
      if(riskSessions.length>6) riskSessions.pop();
    }
  }
  atRisk = riskSessions.filter(s=>s.label==='HIGH').length;
  const atRiskEl = document.getElementById('atRiskCount');
  if(atRiskEl) atRiskEl.textContent = atRisk;
  el.innerHTML = riskSessions.map(s=>`
    <div class="session-item">
      <span class="si-prob si-${s.label==='HIGH'?'high':'med'}">${s.label} ${s.prob}%</span>
      <div class="si-body">
        <div class="si-sess">${s.id}</div>
        <div class="si-detail">${s.events} events · ${s.src}${s.cart?' · 🛒 has cart':''}</div>
      </div>
      <button class="si-btn" onclick="alert('🎯 Sending 10% discount push to ${s.id}!')">💸 Send Promo</button>
    </div>`).join('');
}

// ═══════════════════════════════════════════
// REVENUE REALTIME CHART
// ═══════════════════════════════════════════
let revRealtimeChart = null;
const revLabels = Array(30).fill('');

function initRevenueRealtime(){
  revRealtimeChart = mkChart('revenueRealtime',{
    type:'line',
    data:{
      labels: revLabels,
      datasets:[{
        label:'Revenue (IDR K)',
        data: [...revenueHistory],
        borderColor:'#6366f1',
        backgroundColor:'rgba(99,102,241,0.08)',
        fill:true, tension:.4, pointRadius:0, borderWidth:2,
      }]
    },
    options:{
      animation:false,
      plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>' ₹'+c.raw.toLocaleString()+'K'}}},
      scales:{
        x:{display:false},
        y:{grid:{color:'rgba(255,255,255,0.04)'},ticks:{callback:v=>'₹'+v+'K'}}
      }
    }
  });
}

// ═══════════════════════════════════════════
// HEATMAP
// ═══════════════════════════════════════════
function buildHeatmap(){
  const wrap = document.getElementById('heatmapWrap');
  if(!wrap) return;
  const maxActivity = [20,60,30,25,40,70,150,300,450,500,480,420,520,490,430,460,500,520,480,400,350,280,180,80];
  const max = Math.max(...maxActivity);
  wrap.innerHTML = maxActivity.map((v,i)=>{
    const pct = v/max;
    const alpha = 0.08 + pct*0.75;
    const color = `rgba(99,102,241,${alpha.toFixed(2)})`;
    const border = `rgba(99,102,241,${(alpha*1.4).toFixed(2)})`;
    return `<div class="hm-cell" style="background:${color};border:1px solid ${border}" title="${i}:00 — ${v} events/min">${i}h</div>`;
  }).join('');
}

// ═══════════════════════════════════════════
// TRAFFIC DONUT (ops page)
// ═══════════════════════════════════════════
function initTrafficDonut(){
  mkChart('trafficDonut',{
    type:'doughnut',
    data:{labels:['Android','iOS','Web'],
      datasets:[{data:[68.6,21.4,10],backgroundColor:['#6366f1','#06b6d4','#10b981'],borderWidth:2,borderColor:'#0d0d17'}]
    },
    options:{cutout:'68%',plugins:{legend:{display:false}}}
  });
}

// ═══════════════════════════════════════════
// MAIN LIVE SIMULATION LOOP
// ═══════════════════════════════════════════
function liveLoop(){
  // Generate order
  const o = randomOrder();
  const revBump  = o.status==='success' ? Math.floor(Math.random()*600+200)*1000 : 0;

  // Update state
  revenue += revBump;
  if(o.status==='success'){ orders++; successOrders++; }
  else if(o.status==='fail'){ orders++; }
  else { abandoned++; }
  sessions = Math.max(800, sessions + Math.floor(Math.random()*6 - 2));

  // Feed + sidebar
  addOrderToFeed(o);
  const revK = Math.round(revenue/1000);
  const revDisp = revenue>=1e9 ? `₹${(revenue/1e9).toFixed(2)}B` : revenue>=1e6 ? `₹${(revenue/1e6).toFixed(1)}M` : `₹${revK}K`;
  const sbRev = document.getElementById('sb-revenue'); if(sbRev) sbRev.textContent = revDisp;
  const heroRev = document.getElementById('heroRev');  if(heroRev) heroRev.textContent = revDisp;
  const rollingEl = document.getElementById('revenueRolling'); if(rollingEl) rollingEl.textContent = revDisp;
  setEl('tb-users', sessions.toLocaleString());
  setEl('kpi-orders', orders.toLocaleString());
  setEl('kpi-success', successOrders.toLocaleString());
  setEl('kpi-abandoned', abandoned.toLocaleString());
  setEl('kpi-sessions', sessions.toLocaleString());
  setEl('liveOrders', orders.toLocaleString());
  setEl('liveUsers', sessions.toLocaleString());

  // Revenue chart
  revenueHistory.push(revK);
  revenueHistory.shift();
  if(revRealtimeChart){
    revRealtimeChart.data.datasets[0].data = [...revenueHistory];
    revRealtimeChart.update('none');
  }

  // Sparklines
  sparkHistory.orders.push(orders); sparkHistory.orders.shift();
  sparkHistory.success.push(successOrders); sparkHistory.success.shift();
  sparkHistory.abandon.push(abandoned); sparkHistory.abandon.shift();
  sparkHistory.sessions.push(sessions); sparkHistory.sessions.shift();
  drawSpark('spark-orders', sparkHistory.orders, '#6366f1');
  drawSpark('spark-success', sparkHistory.success, '#10b981');
  drawSpark('spark-abandon', sparkHistory.abandon, '#f59e0b');
  drawSpark('spark-sessions', sparkHistory.sessions, '#ec4899');

  // Risk sessions
  refreshRiskSessions();
}

function setEl(id, val){
  const el = document.getElementById(id);
  if(el) el.textContent = val;
}

// Start loop
setInterval(liveLoop, 900);

// ═══════════════════════════════════════════
// REVENUE PAGE
// ═══════════════════════════════════════════
const monthlyRaw = [
  {m:'Jul-16',r:150,g:12198,c:151},{m:'Aug-16',r:281,g:87,c:432},{m:'Sep-16',r:423,g:50.9,c:855},
  {m:'Oct-16',r:521,g:23,c:1376},{m:'Nov-16',r:597,g:14.6,c:1973},{m:'Dec-16',r:601,g:0.6,c:2574},
  {m:'Jan-17',r:749,g:24.8,c:3323},{m:'Mar-17',r:1059,g:31.6,c:5186},{m:'Jul-17',r:1778,g:46.3,c:10551},
  {m:'Jan-18',r:2277,g:2.6,c:22823},{m:'Jul-18',r:3579,g:26.9,c:40015},{m:'Dec-18',r:4112,g:4.3,c:58990},
  {m:'Jan-19',r:4261,g:3.6,c:63252},{m:'Jul-19',r:5475,g:14,c:91361},{m:'Dec-19',r:5850,g:-1,c:120062},
  {m:'Jan-20',r:6215,g:6.2,c:126277},{m:'Jul-20',r:7804,g:8.8,c:168289},{m:'Dec-20',r:9216,g:3.4,c:211541},
  {m:'Jan-21',r:9604,g:4.2,c:221145},{m:'Jul-21',r:11532,g:11.7,c:282894},{m:'Dec-21',r:13453,g:1.8,c:346845},
  {m:'Jan-22',r:14334,g:6.6,c:361180},{m:'Mar-22',r:15145,g:14.5,c:389553},{m:'May-22',r:15719,g:1.6,c:420738},{m:'Jul-22',r:13198,g:-12.4,c:448997},
];
let revMode = 'revenue';
let mainRevCh = null;

function initRevenuePage(){
  mainRevCh = mkChart('mainRevChart', buildRevCfg('revenue'));
  mkChart('payMethodChart',{type:'doughnut',data:{labels:['Credit Card','GoPay','OVO','Debit Card','LinkAja'],datasets:[{data:[44,25,24,20,11],backgroundColor:P,borderWidth:2,borderColor:'#111122'}]},options:{cutout:'55%',plugins:{legend:{position:'bottom'}}}});
  mkChart('segRevChart',{type:'doughnut',data:{labels:['VIP (5+ orders)','Regular','Occasional'],datasets:[{data:[424852,12596,11549],backgroundColor:['#6366f1','#10b981','#f59e0b'],borderWidth:2,borderColor:'#111122'}]},options:{cutout:'55%',plugins:{legend:{position:'bottom'},tooltip:{callbacks:{label:c=>` ₹${(c.raw/1000).toFixed(0)}B IDR`}}}}});
  const hours = Array.from({length:24},(_,i)=>i+':00');
  const activity = [20,60,30,25,40,70,150,300,450,500,480,420,520,490,430,460,500,520,480,400,350,280,180,80];
  mkChart('peakHrsChart',{type:'bar',data:{labels:hours,datasets:[{label:'Transactions',data:activity,backgroundColor:hours.map((_,i)=>i>=10&&i<=20?'rgba(99,102,241,0.7)':'rgba(99,102,241,0.25)'),borderRadius:3}]},options:{plugins:{legend:{display:false}},scales:{x:{ticks:{maxTicksLimit:8,font:{size:9}}},y:{grid:{color:'rgba(255,255,255,0.04)'}}}}});
}

function buildRevCfg(mode){
  const labels = monthlyRaw.map(d=>d.m);
  let data, label, color, type='line';
  if(mode==='revenue'){data=monthlyRaw.map(d=>d.r);label='Revenue (IDR M)';color='#6366f1';}
  else if(mode==='growth'){data=monthlyRaw.map(d=>d.g);label='MoM Growth (%)';color='#10b981';type='bar';}
  else{data=monthlyRaw.map(d=>d.c);label='Cumulative (IDR M)';color='#06b6d4';}
  return{
    type,data:{labels,datasets:[{label,data,borderColor:color,
      backgroundColor:type==='line'?color+'12':data.map(v=>v>=0?'rgba(16,185,129,0.6)':'rgba(239,68,68,0.6)'),
      fill:type==='line',tension:.4,borderWidth:2,pointRadius:2,borderRadius:4}]},
    options:{responsive:true,plugins:{legend:{display:false}},scales:{x:{ticks:{maxTicksLimit:15,font:{size:10}},grid:{display:false}},y:{grid:{color:'rgba(255,255,255,0.04)'}}}}
  };
}
window.revToggle = function(btn, mode){
  revMode = mode;
  document.querySelectorAll('.tgl').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  if(mainRevCh){ mainRevCh.destroy(); mainRevCh = mkChart('mainRevChart', buildRevCfg(mode)); }
};
window.updateRevenuePage = function(){};

// ═══════════════════════════════════════════
// CUSTOMERS PAGE
// ═══════════════════════════════════════════
const ltvRows = [
  {rank:1,name:'Siti Suartini',orders:550,ltv:'₹320.1M',aov:'₹581,931',since:'Jul 2016',seg:'Power Buyer'},
  {rank:2,name:'Eva Usada',orders:505,ltv:'₹297.7M',aov:'₹589,445',since:'Dec 2016',seg:'Power Buyer'},
  {rank:3,name:'Tari Wastuti',orders:503,ltv:'₹256.5M',aov:'₹509,899',since:'Jan 2017',seg:'Power Buyer'},
  {rank:4,name:'Juli Winarsih',orders:370,ltv:'₹248.7M',aov:'₹672,079',since:'Apr 2017',seg:'Regular'},
  {rank:5,name:'Queen Mandasari',orders:460,ltv:'₹239.1M',aov:'₹519,834',since:'May 2017',seg:'Power Buyer'},
  {rank:6,name:'Paramita Handayani',orders:416,ltv:'₹238.8M',aov:'₹574,145',since:'Jul 2017',seg:'Regular'},
  {rank:7,name:'Eva Usada II',orders:458,ltv:'₹236.5M',aov:'₹516,363',since:'Feb 2017',seg:'Power Buyer'},
  {rank:8,name:'Wirda Utami',orders:356,ltv:'₹222.5M',aov:'₹624,898',since:'Mar 2018',seg:'Regular'},
  {rank:9,name:'Ani Agustina',orders:391,ltv:'₹219.0M',aov:'₹560,106',since:'Jan 2017',seg:'Regular'},
  {rank:10,name:'Intan Sudiati',orders:353,ltv:'₹217.1M',aov:'₹615,088',since:'Sep 2017',seg:'Regular'},
  {rank:11,name:'Eva Wahyuni',orders:399,ltv:'₹209.7M',aov:'₹525,604',since:'Mar 2018',seg:'Regular'},
  {rank:12,name:'Putu Wasita',orders:318,ltv:'₹208.0M',aov:'₹654,025',since:'Mar 2018',seg:'Big-Ticket'},
  {rank:13,name:'Dacin Waskita',orders:379,ltv:'₹203.8M',aov:'₹537,622',since:'Feb 2017',seg:'Regular'},
  {rank:14,name:'Kezia Rahmawati',orders:436,ltv:'₹201.5M',aov:'₹462,188',since:'Oct 2017',seg:'Casual'},
  {rank:15,name:'Hartaka Wijaya',orders:383,ltv:'₹201.4M',aov:'₹525,748',since:'Aug 2016',seg:'Regular'},
];
const segColors = {'Power Buyer':'#f59e0b','Regular':'#10b981','Big-Ticket':'#ec4899','Casual':'#6366f1'};

function initCustomersPage(){
  const tbody = document.getElementById('ltvTbody');
  if(tbody) tbody.innerHTML = ltvRows.map((r,i)=>`
    <tr>
      <td><strong style="color:${i<3?'#f59e0b':'#9ea3c8'}">#${r.rank}</strong></td>
      <td><strong style="color:#e8eaf6">${r.name}</strong></td>
      <td>${r.orders}</td>
      <td><strong style="color:#10b981">${r.ltv}</strong></td>
      <td>${r.aov}</td>
      <td style="color:#5a6080">${r.since}</td>
      <td><span class="pill" style="background:${segColors[r.seg]||'#6366f1'}22;color:${segColors[r.seg]||'#6366f1'}">${r.seg}</span></td>
    </tr>`).join('');

  mkChart('clusterBarChart',{type:'bar',data:{labels:['Casual','Budget','Power','Regular','Big-Ticket'],datasets:[{label:'Customers',data:[31397,2539,1827,12193,2286],backgroundColor:['#6366f1cc','#06b6d4cc','#f59e0bcc','#10b981cc','#ec4899cc'],borderRadius:6}]},options:{plugins:{legend:{display:false}},scales:{x:{grid:{display:false}},y:{grid:{color:'rgba(255,255,255,0.04)'}}}}});
  mkChart('ltvBarChart',{type:'bar',indexAxis:'y',data:{labels:ltvRows.slice(0,10).map(r=>r.name.split(' ')[0]),datasets:[{label:'LTV (IDR M)',data:ltvRows.slice(0,10).map(r=>parseFloat(r.ltv.replace(/[₹M,]/g,''))),backgroundColor:[...Array(3).fill('#f59e0bcc'),...Array(7).fill('#6366f1cc')],borderRadius:4}]},options:{plugins:{legend:{display:false}},scales:{x:{grid:{color:'rgba(255,255,255,0.04)'},ticks:{callback:v=>v+'M'}},y:{grid:{display:false},ticks:{font:{size:11}}}}}});
}

// ═══════════════════════════════════════════
// CONVERSION PAGE
// ═══════════════════════════════════════════
function initConversionPage(){
  const promoCodes = ['No Promo','AZ2022','BUYMORE','WEEKENDSERU','XX2022','LIBURDONG'];
  const promoTxns  = [526048,89227,66835,61941,44744,20965];
  mkChart('promoChart',{type:'bar',data:{labels:promoCodes,datasets:[{label:'Transactions',data:promoTxns,backgroundColor:P.map(c=>c+'cc'),borderRadius:6},{label:'Avg AOV (₹K)',data:[551,548,546,547,544,553],backgroundColor:'rgba(255,255,255,0.08)',borderRadius:6,yAxisID:'y2'}]},options:{plugins:{legend:{position:'bottom'}},scales:{x:{grid:{display:false},ticks:{font:{size:10}}},y:{grid:{color:'rgba(255,255,255,0.04)'}},y2:{position:'right',grid:{display:false},ticks:{callback:v=>'₹'+v+'K'}}}}});
  // Animate funnel bars
  document.querySelectorAll('.fs-fill').forEach(el=>{
    el.style.width = '0%';
    setTimeout(()=>{el.style.width = getComputedStyle(el.parentElement.parentElement).getPropertyValue('--fw');},100);
  });
}

// ═══════════════════════════════════════════
// ML PAGE
// ═══════════════════════════════════════════
const allSessions = Array.from({length:12},(_,i)=>({
  id:'sess_'+Math.random().toString(36).slice(2,8),
  prob: Math.round(Math.random()*100),
}));

function refreshMLScores(){
  allSessions.forEach(s=>{ s.prob = Math.round(Math.random()*100); });
  renderMLGrid();
  updateMLDonut();
}
window.refreshMLScores = refreshMLScores;

function renderMLGrid(){
  const el = document.getElementById('mlScoreGrid');
  if(!el) return;
  el.innerHTML = allSessions.map(s=>{
    const cl = s.prob>=70?'high':s.prob>=40?'med':'low';
    const lb = s.prob>=70?'HIGH_CONVERSION':s.prob>=40?'MED_CONVERSION':'LOW_CONVERSION';
    const lbCl = s.prob>=70?'lb-high':s.prob>=40?'lb-med':'lb-low';
    return `<div class="ml-score-item">
      <div class="msi-sess">${s.id}</div>
      <div class="msi-prob msi-${cl}">${s.prob}%</div>
      <span class="msi-label ${lbCl}">${lb}</span>
    </div>`;
  }).join('');
}

let mlDonutCh = null;
function updateMLDonut(){
  const h = allSessions.filter(s=>s.prob>=70).length;
  const m = allSessions.filter(s=>s.prob>=40&&s.prob<70).length;
  const l = allSessions.filter(s=>s.prob<40).length;
  if(mlDonutCh){
    mlDonutCh.data.datasets[0].data = [h,m,l];
    mlDonutCh.update();
  } else {
    mlDonutCh = mkChart('mlPredDonut',{
      type:'doughnut',
      data:{labels:['HIGH (≥70%)','MEDIUM (40-69%)','LOW (<40%)'],datasets:[{data:[h,m,l],backgroundColor:['#10b981','#f59e0b','#ef4444'],borderWidth:2,borderColor:'#0d0d17'}]},
      options:{cutout:'60%',plugins:{legend:{position:'bottom'},tooltip:{callbacks:{label:c=>` ${c.label}: ${c.raw} sessions`}}}}
    });
  }
}

// Interactive scorer
window.score = function(){
  const ev   = parseInt(document.getElementById('si-ev').value);
  const view = document.getElementById('si-view').checked;
  const cart = document.getElementById('si-cart').checked;
  const srch = document.getElementById('si-srch').checked;
  const promo= document.getElementById('si-promo').checked;
  document.getElementById('si-ev-v').textContent = ev;
  let p = Math.min(ev/50,1)*0.3;
  if(view)  p+=0.15; if(cart)  p+=0.28; if(srch) p+=0.10; if(promo) p+=0.20;
  p = Math.max(0,Math.min(p + (Math.random()*0.04-0.02),1));
  const pct = Math.round(p*100);
  // Animate arc
  const arc = document.getElementById('meterArc');
  const pctEl = document.getElementById('meterPct');
  if(arc){
    const offset = 267 - (267 * pct/100);
    arc.style.strokeDashoffset = offset;
    pctEl.textContent = pct+'%';
  }
  const lbl = document.getElementById('scorerLabel');
  const act = document.getElementById('scorerAction');
  if(pct>=70){
    if(lbl){lbl.style.color='#10b981';lbl.textContent='🟢 HIGH CONVERSION';}
    if(act) act.textContent='Save marketing budget — this user is on track to buy naturally.';
  } else if(pct>=40){
    if(lbl){lbl.style.color='#f59e0b';lbl.textContent='🟡 MEDIUM CONVERSION';}
    if(act) act.textContent='Send a 10% discount push notification NOW to secure the sale.';
  } else {
    if(lbl){lbl.style.color='#ef4444';lbl.textContent='🔴 LOW CONVERSION';}
    if(act) act.textContent='Passive scroller. Enroll in a re-engagement email sequence.';
  }
};

const recData = {
  power:[{icon:'👟',n:'Nike Air Max 270',c:'Footwear / Sports',s:'4.82'},{icon:'🎽',n:'Under Armour Training Kit',c:'Apparel / Sportswear',s:'4.71'},{icon:'👜',n:'Puma Sports Gym Bag',c:'Accessories / Bags',s:'4.63'},{icon:'🕶️',n:'Ray-Ban UV Sport',c:'Accessories / Eyewear',s:'4.57'},{icon:'⌚',n:'Garmin Forerunner 55',c:'Accessories / Watches',s:'4.49'}],
  casual:[{icon:'👗',n:'H&M Floral Summer Dress',c:'Apparel / Dresses',s:'3.91'},{icon:'👡',n:'Steve Madden Block Heels',c:'Footwear / Heels',s:'3.78'},{icon:'👛',n:'Zara Mini Crossbody',c:'Accessories / Bags',s:'3.65'},{icon:'🧣',n:'Forever 21 Knit Scarf',c:'Accessories / Scarves',s:'3.52'},{icon:'💍',n:'Mango Gold Bracelet',c:'Accessories / Jewellery',s:'3.41'}],
  bigticket:[{icon:'⌚',n:'Seiko Presage Automatic',c:'Accessories / Watches',s:'4.95'},{icon:'👜',n:'Longchamp Le Pliage Tote',c:'Accessories / Bags',s:'4.88'},{icon:'🧥',n:'Calvin Klein Wool Overcoat',c:'Apparel / Outerwear',s:'4.79'},{icon:'👠',n:'Christian Louboutin Court',c:'Footwear / Heels',s:'4.73'},{icon:'🕶️',n:'Tom Ford Titanium Frames',c:'Accessories / Eyewear',s:'4.68'}],
};

window.loadRecs = function(){
  const v = document.getElementById('recSel').value;
  const items = recData[v]||recData.casual;
  const el = document.getElementById('recShelf');
  if(el) el.innerHTML = items.map(r=>`
    <div class="rec-item">
      <div class="rec-item-icon">${r.icon}</div>
      <div class="rec-item-name">${r.n}</div>
      <div class="rec-item-cat">${r.c}</div>
      <div class="rec-item-score">⭐ ${r.s} match</div>
    </div>`).join('');
};

function initMLPage(){
  refreshMLScores();
  updateMLDonut();
  loadRecs();
  score();
  mkChart('modelCompareChart',{type:'bar',data:{labels:['Logistic Reg.','Random Forest','GBT'],datasets:[{label:'AUC',data:[0.5031,0.4926,0.5226],backgroundColor:'#6366f1cc',borderRadius:4},{label:'F1 Score',data:[0.7367,0.9314,0.7259],backgroundColor:'#10b981cc',borderRadius:4},{label:'Accuracy',data:[0.6301,0.9539,0.6160],backgroundColor:'#f59e0bcc',borderRadius:4}]},options:{responsive:true,plugins:{legend:{position:'bottom'}},scales:{y:{min:0,max:1.05,grid:{color:'rgba(255,255,255,0.04)'}},x:{grid:{display:false}}}}});
}

// ═══════════════════════════════════════════
// PRODUCTS PAGE
// ═══════════════════════════════════════════
const TREND_PRODS = [
  {icon:'👟',n:'Nike Air Max 270',v:'2,341 views',badge:'🔥 HOT',bc:'trend-hot'},
  {icon:'👗',n:'Adidas Polo Shirt',v:'1,987 views',badge:'↑ Rising',bc:'trend-up'},
  {icon:'👜',n:'Puma Sports Bag',v:'1,654 views',badge:'↑ Rising',bc:'trend-up'},
  {icon:'🕶️',n:'Ray-Ban Classic',v:'1,432 views',badge:'↑ Rising',bc:'trend-up'},
  {icon:'🧥',n:'Zara Wool Coat',v:'1,210 views',badge:'🔥 HOT',bc:'trend-hot'},
  {icon:'👞',n:'Clarks Derby Shoes',v:'1,089 views',badge:'↑ Rising',bc:'trend-up'},
  {icon:'💍',n:'Swarovski Ring Set',v:'987 views',badge:'🔥 HOT',bc:'trend-hot'},
  {icon:'🎽',n:'Under Armour Shorts',v:'876 views',badge:'↑ Rising',bc:'trend-up'},
  {icon:'⌚',n:'Fossil Minimalist Watch',v:'765 views',badge:'🔥 HOT',bc:'trend-hot'},
  {icon:'🧣',n:'Gucci Silk Scarf',v:'654 views',badge:'↑ Rising',bc:'trend-up'},
];

function initProductsPage(){
  const tg = document.getElementById('trendingGrid');
  if(tg) tg.innerHTML = TREND_PRODS.map(p=>`
    <div class="trend-item">
      <div class="trend-icon">${p.icon}</div>
      <div class="trend-name">${p.n}</div>
      <div class="trend-views">${p.views||p.v}</div>
      <span class="trend-badge ${p.bc}">${p.badge}</span>
    </div>`).join('');

  mkChart('catRevChart',{type:'bar',data:{labels:['Apparel','Accessories','Footwear','Personal Care','Sporting Goods'],datasets:[{label:'Revenue (IDR B)',data:[198,142,89,45,21],backgroundColor:P.map(c=>c+'cc'),borderRadius:6}]},options:{plugins:{legend:{display:false}},scales:{x:{grid:{display:false}},y:{grid:{color:'rgba(255,255,255,0.04)'},ticks:{callback:v=>v+'B'}}}}});
  mkChart('catSplitChart',{type:'doughnut',data:{labels:['Apparel 48%','Accessories 26%','Footwear 21%','Other 5%'],datasets:[{data:[48,26,21,5],backgroundColor:P,borderWidth:2,borderColor:'#111122'}]},options:{cutout:'60%',plugins:{legend:{position:'bottom'}}}});
}

// Update trending views every few seconds
setInterval(()=>{
  TREND_PRODS.forEach(p=>{
    const n = parseInt((p.v||'1000').replace(/[^0-9]/g,''));
    const bump = Math.floor(Math.random()*5);
    const newN = n+bump;
    p.v = newN.toLocaleString()+' views';
  });
  const tg = document.getElementById('trendingGrid');
  if(tg&&tg.offsetParent) tg.innerHTML = TREND_PRODS.map(p=>`
    <div class="trend-item">
      <div class="trend-icon">${p.icon}</div>
      <div class="trend-name">${p.n}</div>
      <div class="trend-views">${p.v}</div>
      <span class="trend-badge ${p.bc}">${p.badge}</span>
    </div>`).join('');
}, 3000);

// ═══════════════════════════════════════════
// ALERTS PAGE
// ═══════════════════════════════════════════
const alertsData = [
  {title:'Cart→Checkout Drop Critical',body:'42% of sessions with cart items are NOT proceeding to checkout. This is the single largest revenue leak in your funnel.',time:'2 min ago',color:'var(--red)',tag:'🔴 Critical',tcls:'pill red-pill'},
  {title:'High-Risk Session Wave',body:'23 sessions currently scored MEDIUM conversion by ML model. Sending targeted discounts could recover ~₹12M in potential revenue.',time:'5 min ago',color:'var(--amber)',tag:'🟡 Warning',tcls:'pill warn-pill'},
  {title:'LinkAja Success Rate Dip',body:'LinkAja payment success rate dropped to 95.2% in the last hour (avg 95.5%). Monitor for further decline — may indicate gateway issue.',time:'11 min ago',color:'var(--amber)',tag:'🟡 Warning',tcls:'pill warn-pill'},
  {title:'Power Buyer Session Active',body:'Customer ID 43202 (Siti Suartini, LTV ₹320M) is currently browsing. Personal stylist notification opportunity.',time:'1 min ago',color:'var(--violet)',tag:'💎 VIP',tcls:'pill purple'},
  {title:'Apparel → Footwear Cross-sell',body:'Market basket analysis detected 68% confidence for Apparel/Footwear pair. Show "Complete the look" banner to 3,400 active Apparel viewers.',time:'18 min ago',color:'var(--indigo)',tag:'💡 Insight',tcls:'pill indigo-pill'},
  {title:'Weekend Revenue Surge Predicted',body:'MoM trend analysis shows Saturday/Sunday generates 18% more revenue. Pre-load WEEKENDSERU promo activation for Friday 20:00 WIB.',time:'32 min ago',color:'var(--emerald)',tag:'📈 Opportunity',tcls:'pill green'},
];

const actionsData = [
  {icon:'💸',title:'Send 10% Discount to 23 Medium-Risk Sessions',desc:'ML model identified 23 sessions on the fence. Targeted push notification could convert ₹12-15M in revenue.',btn:'Send Now',primary:true},
  {icon:'🛒',title:'Cart Abandonment Recovery — 1,847 users',desc:'Users with items in cart who left 30 min ago. Email recovery campaign with 5% discount has 34% open rate.',btn:'Launch Campaign',primary:true},
  {icon:'👑',title:'Notify VIP Buyer: Siti Suartini is Online',desc:'Your #1 LTV customer (₹320M) is active. Assign personal stylist and show premium new arrivals.',btn:'Alert Team',primary:false},
  {icon:'🔗',title:'Show "Complete the Look" to 3,400 Apparel Browsers',desc:'Market basket: Apparel→Footwear confidence 68%. Cross-sell banner on product pages could add ₹2.1M/day.',btn:'Activate Banner',primary:false},
  {icon:'📅',title:'Schedule WEEKENDSERU Promo for Friday 20:00',desc:'Historical data shows weekend evenings peak. Pre-schedule promo auto-activation for maximum impact.',btn:'Schedule',primary:false},
];

function initAlertsPage(){
  const ag = document.getElementById('alertsGrid');
  if(ag) ag.innerHTML = alertsData.map(a=>`
    <div class="alert-card" style="--alc:${a.color}">
      <div class="alc-row"><div class="alc-title">${a.title}</div><div class="alc-time">${a.time}</div></div>
      <div class="alc-body">${a.body}</div>
      <span class="alc-tag" style="background:${a.color}20;color:${a.color};border:1px solid ${a.color}40">${a.tag}</span>
    </div>`).join('');

  const ac = document.getElementById('actionCenter');
  if(ac) ac.innerHTML = actionsData.map(a=>`
    <div class="ac-item">
      <div class="ac-icon">${a.icon}</div>
      <div class="ac-body"><div class="ac-title">${a.title}</div><div class="ac-desc">${a.desc}</div></div>
      <button class="ac-btn ${a.primary?'':'secondary'}" onclick="alert('✅ Action executed: ${a.title.substring(0,30)}...')">⚡ ${a.btn}</button>
    </div>`).join('');

  // Populate anomaly feed
  const af = document.getElementById('anomalyFeed');
  if(af){
    const anomSessions = Array.from({length:8},()=>({
      id:'sess_'+Math.random().toString(36).slice(2,8),
      events:Math.floor(Math.random()*20)+29,
      time:Math.floor(Math.random()*30)+'m ago'
    }));
    af.innerHTML = anomSessions.map(s=>`
      <div class="anm-item">
        <span class="anm-badge">🚨 ANOMALY</span>
        <div class="anm-body">${s.id} — <strong>${s.events} events</strong> in session (threshold: 28)</div>
        <div class="anm-time">${s.time}</div>
      </div>`).join('');
  }
}

// ═══════════════════════════════════════════
// PIPELINE PAGE
// ═══════════════════════════════════════════
const modMsgs = {
  ingestion:['▶ Starting ingestion...','📂 Scanning data/raw/ for Kaggle dataset...','⚠ Kaggle dataset not found — deploying synthetic data generator...','🔧 Running generate_sample_data.py...','  ✓ Generated customer.csv (5,000 rows)','  ✓ Generated product.csv (2,000 rows)','  ✓ Generated transactions.csv (15,000 rows)','  ✓ Generated click_stream.csv (80,000 rows)','💾 Converting CSV → Parquet...','✅ Ingestion complete. Files saved to data/processed/'],
  eda:['▶ Starting EDA...','📂 Loading Parquet from data/processed/...','🔍 100,000 customers | 64.2% Female | 76.6% Android','🔍 44,000 products | Apparel 48% | Accessories 26%','🔍 850,000 txns | 95.7% success | Peak: Sat/Sun 10am-8pm','🔍 12.8M clickstream events | 14.3 avg events/session','📊 Generating chart: 01_customer_demographics.png ✓','📊 Generating chart: 02_product_analysis.png ✓','📊 Generating chart: 03_transaction_analysis.png ✓','📊 Generating chart: 04_clickstream_analysis.png ✓','📊 Generating chart: 05_cross_table_insights.png ✓','✅ EDA complete → outputs/eda/'],
  transformations:['▶ Initialising Spark: ECommerce-AdvancedSQL','📋 Registering temp views: customers, products, transactions, clickstream','⚡ Query 1: Per-Customer Conversion Funnel (4-Table JOIN + CTEs)...','  → ROW_NUMBER, DENSE_RANK, CASE segmentation','  ✓ Saved: customer_conversion_funnel.csv','⚡ Query 2: Market Basket (LATERAL VIEW EXPLODE + Self-Join)...','  → Co-purchase support/confidence/lift metrics','  ✓ Saved: market_basket_analysis.csv','⚡ Query 3: Cohort Retention (MONTHS_BETWEEN + pivot CASE)...','  ✓ Saved: cohort_retention.csv','⚡ Query 4: RFM Scoring (NTILE quintiles + composite score)...','  ✓ Saved: rfm_scoring.csv','⚡ Query 5: Purchase Velocity (LAG + DATEDIFF)...','  ✓ Saved: purchase_velocity.csv','⚡ Query 6: Product Affinity (4-table JOIN + browse-buy lift)...','  ✓ Saved: product_affinity_network.csv','✅ ALL 6 SQL QUERIES COMPLETE'],
  ml_pipeline:['▶ Initialising Spark: ECommerce-AdvancedML','── TASK 1: CLICKSTREAM-ENHANCED PAYMENT CLASSIFICATION ──','  Features: 15 total (txn:4 + clickstream:8 + demographics:3)','  Train: 40,000 | Test: 10,000 | Class weight Failed: ~11x','  Training Logistic Regression...  AUC:0.5031 F1:0.7367 Acc:63.0%','  Training Random Forest (30 trees, depth 8)...  AUC:0.4926 F1:0.9314 Acc:95.4%','  Training GBT (15 iterations)...  AUC:0.5226 F1:0.7259 Acc:61.6%','  ✓ Saved: outputs/ml/classification_results.json','── TASK 2: ALS COLLABORATIVE FILTERING ──','  Customer×Product interaction matrix: implicit ratings (view=1 → checkout=5)','  ALS rank=10, regParam=0.1, maxIter=10','  RMSE on test: 0.8243 | Generating top-5 recs per customer...','  ✓ Saved: outputs/ml/als_recommendations.csv','── TASK 3: RFM + BEHAVIORAL KMEANS CLUSTERING ──','  Features: RFM(6) + browsing(4) = 10 | k search: [3,4,5,6,8]','  k=3 sil=0.494 | k=4 sil=0.563 | k=5 sil=0.629 ← BEST','  Final model k=5: Casual(31K) Budget(2.5K) Power(1.8K) Regular(12K) BigTicket(2.3K)','  ✓ Saved: outputs/ml/rfm_clustering_results.json','✅ ADVANCED ML PIPELINE COMPLETE'],
  streaming:['▶ Advanced Structured Streaming starting...','🌊 Stream Simulator → writing JSON files to data/stream_input/','  Batch 010: 100 events ✓','  Batch 020: 200 events ✓','  Batch 030: 300 events ✓','  Batch 050: 500 events ✓ (complete)','📦 Static product catalog: 44,000 products loaded','✓ Stream-Static JOIN configured: events × product catalog','⏳ Streaming 60s | Queries: category_trending | session_scores','  [Batch 1] +47 sessions scored | Category trending: Apparel 40%','  [Batch 2] +62 sessions | Anomaly threshold: >28.4 events','  🚨 Anomalous sessions: sess_a82bcd (31ev), sess_f19abc (35ev)','✅ STREAMING PIPELINE COMPLETE → outputs/streaming/'],
  streaming_predictions:['▶ Streaming ML Predictions starting...','── PHASE 1: BATCH TRAINING ──','  Loading historical clickstream + transactions...','  Session features: total_events, viewed_product, added_to_cart, searched,','                    applied_promo, traffic_source_idx, promo_usage_rate','  Training RandomForest (numTrees=50, maxDepth=6, seed=42)...','  Model retained IN-MEMORY — bypasses Hadoop NativeIO Windows bug ✓','  No disk write, no UnsatisfiedLinkError on Windows ✓','── PHASE 2: STREAMING INFERENCE (foreachBatch) ──','  maxFilesPerTrigger=5 | outputMode=append | watermark=10min','  Using vector_to_array() — JVM-native, no Python UDF crash ✓','  [Batch 1] 23 sessions: HIGH=8 MEDIUM=10 LOW=5','  [Batch 2] 31 sessions: HIGH=11 MEDIUM=14 LOW=6','  [Batch 3] 28 sessions: HIGH=9 MEDIUM=13 LOW=6','  Actionable: 24 MEDIUM sessions → 10% discount push triggered','✅ STREAMING ML PREDICTIONS COMPLETE'],
};

window.runMod = function(mod){
  const bar = document.getElementById('modbar-'+mod);
  const stt = document.getElementById('modstt-'+mod);
  if(bar){ bar.style.background='linear-gradient(90deg,#f59e0b,#f59e0b80)'; bar.style.animation='progbar 3s linear'; }
  if(stt){ stt.textContent='RUNNING'; stt.style.color='#f59e0b'; stt.style.background='rgba(245,158,11,0.1)'; }
  const term = document.getElementById('terminal');
  if(term){ term.innerHTML=''; addTL('prompt','$ python -m src.'+mod); }
  const msgs = modMsgs[mod]||['▶ Running...','✅ Done'];
  let i=0;
  const run=()=>{
    if(i>=msgs.length){
      if(bar){ bar.style.background='#10b981'; bar.style.animation='none'; }
      if(stt){ stt.textContent='DONE'; stt.style.color='#10b981'; stt.style.background='rgba(16,185,129,0.1)'; }
      return;
    }
    const m=msgs[i++];
    const type = m.startsWith('✅')?'success':m.startsWith('⚠')?'warn':m.startsWith('🚨')?'error':m.startsWith('🌊')||m.startsWith('[Batch')?'stream':'info';
    addTL(type,m);
    if(term) term.scrollTop=term.scrollHeight;
    setTimeout(run, 160+Math.random()*120);
  };
  run();
};

window.clearTerm = function(){
  const t=document.getElementById('terminal');
  if(t) t.innerHTML='<div class="tl prompt">$ CcMart Data Pipeline v1.0 — Ready</div>';
};

function addTL(type, text){
  const t=document.getElementById('terminal');
  if(!t) return;
  const d=document.createElement('div');
  d.className='tl '+type; d.textContent=text;
  t.appendChild(d);
}

// ═══════════════════════════════════════════
// LAZY INIT
// ═══════════════════════════════════════════
function lazyInit(pg){
  const fns = {
    ops: ()=>{ initRevenueRealtime(); initTrafficDonut(); buildHeatmap(); },
    revenue: initRevenuePage,
    customers: initCustomersPage,
    conversion: initConversionPage,
    ml: initMLPage,
    products: initProductsPage,
    alerts: initAlertsPage,
  };
  if(fns[pg]) fns[pg]();
}

// ═══════════════════════════════════════════
// BOOT
// ═══════════════════════════════════════════
initDone.add('ops');
lazyInit('ops');

// Delay start of live loop so charts render first
setTimeout(()=>{
  for(let i=0;i<8;i++) liveLoop(); // seed some data
  setInterval(liveLoop, 900);
},200);
