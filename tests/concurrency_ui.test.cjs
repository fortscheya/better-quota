// Synthetic browser acceptance: no live credentials or service writes.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'..'),source=fs.readFileSync(path.join(root,'dashboard.go'),'utf8');
const html=source.slice(source.indexOf('`')+1,source.lastIndexOf('`'))
 .replace('__EMBEDDED_CLIENT__',fs.readFileSync(path.join(root,'embedded-client.js'),'utf8'))
 .replace('__CONCURRENCY_CLIENT__',fs.readFileSync(path.join(root,'concurrency-client.js'),'utf8'))
 .replace('__API_BASE__','/v0/management/plugins/better-quota');
const ids=['account-a','account-b'],settings={max_active:3,active_overrides:{},strategy:'capacity_weighted',floor_5h:10,floor_7d:5,auto_ping:{enabled:false,mode:'daily_5h',times:[],model:'gpt-6-luna'},auto_ping_overrides:{},auto_reset:{enabled:false,patrol_seconds:300,lead_seconds:1800}};
const priorityState={one_shot_auth_id:'',continuous_auth_id:''},priorityControls=[];
const writes=[];let statusReads=0,refreshCalls=0,accountRefreshCalls=0,failNext=false,ignoreNext=false,holdNext=false,release=null;
const resetBase=Math.floor(Date.now()/1000);
function status(){return{version:'0.1.7',config:structuredClone(settings),priority:{...priorityState,target_unavailable:false},gate:{admitted:0,reserved:0},score_period_seconds:30,
 last_discovery:new Date().toISOString(),auto_ping_overrides:structuredClone(settings.auto_ping_overrides),dashboard_accounts:ids.map((id,i)=>({credential:{id,auth_index:'synthetic-'+i,email:i?'second@example.com':'a.long-address+first@example.com'},checked_at:new Date().toISOString(),
 quota:{windows:[{seconds:18000,used_percent:i?80:0,reset_at:resetBase+(i?45*60:3*3600)},{seconds:604800,used_percent:i?50:6,reset_at:resetBase+(i?2*86400:6*86400)}]},quantized_score:i?5000:10000,one_shot_selected:priorityState.one_shot_auth_id===id,continuous_selected:priorityState.continuous_auth_id===id,hard_unavailable:false,auto_reset_selected:false,active:0,
 concurrency_limit:settings.active_overrides[id]||3,concurrency_used:0,concurrency_active:0,concurrency_reserved:0,
 concurrency_available:(settings.active_overrides[id]||3)-(i===0?1:0),concurrency_blocked:i===0?1:0,concurrency_overridden:Object.hasOwn(settings.active_overrides,id)}))}}
async function waitFor(test){const end=Date.now()+5000;while(!test()){if(Date.now()>end)throw Error('Synthetic operation did not start');await new Promise(r=>setTimeout(r,10))}}
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'chrome'});
 try{
  const page=await browser.newPage({viewport:{width:1560,height:1050}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.addInitScript(()=>{const original=setInterval;window.setInterval=(fn,ms,...args)=>{if(ms===3000){window.__automaticRefresh=fn;return -1}return original(fn,ms,...args)}});
  await page.route('http://127.0.0.1:8317/**',async route=>{
   const req=route.request(),url=new URL(req.url());
   if(url.pathname==='/dashboard')return route.fulfill({contentType:'text/html',body:html});
   assert.equal(req.headers().authorization,'Bearer synthetic-only');
   if(url.pathname.endsWith('/status')){statusReads++;return route.fulfill({json:status()})}
   if(url.pathname.endsWith('/refresh')){refreshCalls++;return route.fulfill({status:202,json:{status:'discovery_queued_probe_backoff_preserved'}})}
   if(url.pathname.endsWith('/refresh-account')){const body=req.postDataJSON();assert.equal(body.auth_id,'account-a');accountRefreshCalls++;return route.fulfill({status:202,json:{status:'account_probe_queued'}})}
   if(url.pathname.endsWith('/priority-controls')){const b=req.postDataJSON();priorityControls.push(b);if(b.enabled){if(b.mode==='one_shot')priorityState.one_shot_auth_id=b.auth_id;else priorityState.continuous_auth_id=b.auth_id}else if(b.mode==='one_shot'&&priorityState.one_shot_auth_id===b.auth_id)priorityState.one_shot_auth_id='';else if(b.mode==='continuous'&&priorityState.continuous_auth_id===b.auth_id)priorityState.continuous_auth_id='';return route.fulfill({json:{status:'saved'}})}
   assert.equal(url.pathname,'/v0/management/plugins/better-quota/settings');
   const body=req.postDataJSON();writes.push(structuredClone(body));
   if(holdNext){holdNext=false;await new Promise(r=>{release=r})}
   if(failNext){failNext=false;return route.fulfill({status:500,json:{error:'synthetic_write_failed'}})}
   if(ignoreNext){ignoreNext=false;return route.fulfill({json:{status:'saved'}})}
   if(body.active_overrides)Object.assign(settings.active_overrides,body.active_overrides);
   if(body.strategy)settings.strategy=body.strategy;
   for(const key of ['floor_5h','floor_7d','auto_ping_enabled','auto_ping_mode','auto_ping_times'])if(Object.hasOwn(body,key)){if(key==='auto_ping_enabled')settings.auto_ping.enabled=body[key];else if(key==='auto_ping_mode')settings.auto_ping.mode=body[key];else if(key==='auto_ping_times')settings.auto_ping.times=body[key];else settings[key]=body[key]}
   if(body.auto_ping_overrides)for(const [id,value] of Object.entries(body.auto_ping_overrides)){if(value===null)delete settings.auto_ping_overrides[id];else settings.auto_ping_overrides[id]=value}
   if(body.auto_reset_enabled!==undefined)settings.auto_reset.enabled=body.auto_reset_enabled;
   return route.fulfill({json:{status:'saved'}});
  });
  const row=id=>page.locator('.concurrency-limit').filter({visible:true}).nth(ids.indexOf(id));
  const saved=()=>page.waitForFunction(()=>document.querySelector('#concurrency-feedback').dataset.state==='saved');
  async function open(){await page.goto('http://127.0.0.1:8317/dashboard');await page.locator('#key').fill('synthetic-only');await page.locator('#connect').click();await page.locator('#main').waitFor({state:'visible'});await page.waitForFunction(()=>document.querySelector('#connect').dataset.feedbackState==='success')}
  await open();assert.equal(await page.locator('#max-active').count(),0);assert.equal(await page.locator('#floor-5h').inputValue(),'10');assert.equal(await page.locator('#floor-7d').inputValue(),'5');assert.equal(await page.locator('#ping-accounts .ping-account-row').count(),2);
  const summaryCard=page.locator('#main>section.summary-card');
  const routingCard=page.locator('#routing-settings');
  const autoresetCard=page.locator('#autoreset-settings');
  const concurrencyCard=page.locator('#concurrency-settings');
  assert.equal(await page.locator('#main>section').first().getAttribute('class'),'summary-card');
  assert.equal(await summaryCard.evaluate(node=>getComputedStyle(node).position),'sticky');
  assert.equal(await summaryCard.evaluate(node=>getComputedStyle(node).top),'12px');
  assert.ok((await summaryCard.boundingBox()).height<220);
  assert.equal(await routingCard.locator('h2').innerText(),'路由设置');
  assert.equal(await routingCard.locator('#strategy').count(),1);
  assert.equal(await routingCard.locator('#save').count(),0);
  assert.equal(await autoresetCard.locator('#strategy').count(),0);
  assert.equal(await autoresetCard.locator('#batch-limit').count(),0);
  assert.equal(await autoresetCard.locator('#save').count(),1);
  assert.equal(await concurrencyCard.locator('h2').innerText(),'并发设置');
  assert.equal(await concurrencyCard.locator('#batch-limit').count(),1);
  assert.deepEqual(await page.locator('#main>section').evaluateAll(cards=>cards.map(card=>card.id||card.className)),
   ['summary-card','routing-settings','autoreset-settings','automation-settings','concurrency-settings','account-status']);
  assert.ok(await page.locator('#main>section').evaluateAll(cards=>cards.slice(1).every((card,index)=>card.getBoundingClientRect().top-cards[index].getBoundingClientRect().bottom>=15.5)));
  await page.evaluate(()=>window.scrollTo(0,700));
  await page.waitForTimeout(50);
  assert.ok((await summaryCard.boundingBox()).y<=20);
  await page.evaluate(()=>window.scrollTo(0,0));
  assert.equal(await page.locator('#account-count').innerText(),'2');
  assert.equal(await page.locator('.account-refresh').count(),2);
  assert.match(await page.locator('.account-refresh').first().getAttribute('aria-label'),/刷新 a\.long-address\+first@example\.com/);
  const accountRefreshBefore=accountRefreshCalls;await page.locator('.account-refresh').first().click();await waitFor(()=>accountRefreshCalls>accountRefreshBefore);await page.waitForFunction(()=>document.querySelector('.account-refresh')?.textContent==='已刷新');if(process.argv[2]){await page.evaluate(()=>window.scrollTo(0,700));await page.screenshot({path:process.argv[2]+'-account-feedback-light.png'});await page.evaluate(()=>window.scrollTo(0,0))}assert.equal(await page.locator('.account-refresh').count(),2);
  await page.locator('#select-all').click();assert.equal(await page.locator('#select-all').innerText(),'已全选');await page.locator('#select-none').click();assert.equal(await page.locator('#select-none').innerText(),'已清空');
  await page.locator('#batch-limit').fill('0');await page.locator('#set-all-limit').click();await page.waitForFunction(()=>document.querySelector('#set-all-limit').dataset.feedbackState==='error');assert.equal(await page.locator('#set-all-limit').innerText(),'设置失败');
  assert.deepEqual(await page.locator('#concurrency-breakdown .summary-slot-item').allTextContents(),['已用槽位 0','不可用槽位 1','可用槽位 5']);
  assert.deepEqual(await page.locator('#concurrency-breakdown .summary-slot-dot').evaluateAll(items=>items.map(x=>x.className)),['summary-slot-dot used','summary-slot-dot blocked','summary-slot-dot available']);
  assert.equal(await page.locator('#concurrency-breakdown').evaluate(node=>getComputedStyle(node).display),'flex');
  assert.deepEqual(await page.locator('.pooled-window .quota-percent').allTextContents(),['60%','72%']);
  assert.equal(await page.locator('.pooled-window .quota-track-row').count(),2);
  assert.equal(await page.locator('.pooled-window .quota-heading').count(),0);
  assert.ok(await page.locator('.pooled-window .quota-track-row').evaluateAll(rows=>rows.every(row=>row.querySelector('.quota-percent').getBoundingClientRect().left>row.querySelector('.quota-track').getBoundingClientRect().right)));
  assert.deepEqual(await page.locator('.pooled-window .quota-fill').evaluateAll(items=>items.map(x=>x.style.width)),['60%','72%']);
  assert.deepEqual(await page.locator('.pooled-window .quota-fill').evaluateAll(items=>items.map(x=>x.className)),['quota-fill medium','quota-fill high']);
  assert.deepEqual(await page.locator('.pooled-window .quota-track').evaluateAll(items=>items.map(x=>x.getAttribute('aria-label'))),['5h剩余额度','周剩余额度']);
  assert.equal(await page.locator('.pooled-window .quota-reset').count(),0);
  assert.match(await page.locator('#last-refresh').innerText(),/^最近刷新：/);
  const refreshBefore=refreshCalls,statusBefore=statusReads;await page.locator('#summary-refresh').click();await page.waitForFunction(()=>document.querySelector('#last-refresh').textContent.startsWith('最近刷新：'));await waitFor(()=>statusReads>statusBefore);await page.waitForFunction(()=>document.querySelector('#summary-refresh').dataset.feedbackState==='success');assert.equal(refreshCalls,refreshBefore+1);assert.ok(statusReads>statusBefore);
  assert.equal(await autoresetCard.locator('h2').innerText(),'Autoreset 自动兑换重置');
  assert.equal(await autoresetCard.locator('label[for="patrol"]').innerText(),'重置检查频率（秒）');
  assert.equal(await autoresetCard.locator('label[for="lead"]').innerText(),'重置到期前');
  assert.equal(await autoresetCard.locator('span').filter({hasText:'秒自动兑换'}).count(),1);
  assert.equal(await routingCard.locator('#strategy').evaluate(node=>getComputedStyle(node).paddingRight),'30px');
  assert.equal(await page.locator('#ping-mode').evaluate(node=>getComputedStyle(node).paddingRight),'30px');
  const strategyLabelBox=await routingCard.locator('label[for="strategy"]').boundingBox(),strategyBox=await routingCard.locator('#strategy').boundingBox();assert.ok(strategyBox.y-(strategyLabelBox.y+strategyLabelBox.height)>=5);
  const patrolBox=await page.locator('#patrol').boundingBox(),leadBox=await page.locator('#lead').boundingBox();assert.ok(leadBox.y-(patrolBox.y+patrolBox.height)>=10);
  const selectAllBox=await page.locator('#select-all').boundingBox(),accountPickerBox=await page.locator('#account-picker').boundingBox();assert.ok(accountPickerBox.y-(selectAllBox.y+selectAllBox.height)>=10);
  const legendBox=await page.locator('.slot-legend').boundingBox(),tableBox=await page.locator('#account-status .scroll').boundingBox();assert.ok(tableBox.y-(legendBox.y+legendBox.height)>=6);
  assert.doesNotMatch(await autoresetCard.innerText(),/设置保存到插件|只会选择选中的账号|频率最短 30 秒|关闭 autoreset 时可清空选择/);
  assert.deepEqual(await page.locator('#accounts .account').allTextContents(),['a.long-address+first@example.com','second@example.com']);
  assert.deepEqual(await page.locator('#account-picker span').allTextContents(),['a.long-address+first@example.com','second@example.com']);
  assert.doesNotMatch(await page.locator('#main').innerText(),/auth_index|synthetic-|插件覆盖|↑|account-a|account-b/);
  assert.equal(await page.locator('th').filter({hasText:'路由优先级（手动）'}).count(),0);
  assert.equal(await page.locator('th').filter({hasText:'量化权重（0–10000）'}).count(),1);
  assert.equal(await page.locator('.route-priority').count(),0);
  assert.equal(await page.locator('.priority-control').count(),4);
  assert.deepEqual(await page.locator('#accounts tr td:nth-child(3)').allTextContents(),['10000只读量化分数','5000只读量化分数']);
  assert.deepEqual(await page.locator('.quota-cell .quota-percent').allTextContents(),['100%','94%','20%','50%']);
  assert.deepEqual(await page.locator('.quota-cell .quota-fill').evaluateAll(items=>items.map(x=>x.style.width)),['100%','94%','20%','50%']);
  assert.equal(await page.locator('.quota-cell .quota-fill.high').count(),2);assert.equal(await page.locator('.quota-cell .quota-fill.medium').count(),1);assert.equal(await page.locator('.quota-cell .quota-fill.low').count(),1);
  assert.ok((await page.locator('.quota-cell .quota-reset').allTextContents()).every(x=>/^(?:\d+(?:min|h|d)后|已到期|未知)$/.test(x)));
  assert.equal(await page.locator('.quota-cell .quota-track[role=progressbar][aria-valuenow]').count(),4);
  assert.ok(await page.locator('.quota-cell .window').evaluateAll(windows=>windows.every(w=>w.querySelector('.quota-reset').getBoundingClientRect().top>=w.querySelector('.quota-track').getBoundingClientRect().bottom)));
  assert.equal(await page.evaluate(()=>accountEmail({id:'internal-filename.json'})),'邮箱未知');
  assert.deepEqual(await page.evaluate(()=>{const now=Date.parse('2026-01-01T00:00:00Z');const at=seconds=>Math.floor((now+seconds*1000)/1000);return [30,3599,3600,5*3600,24*3600,7*24*3600].map(seconds=>resetRemainingLabel(at(seconds),now)).concat(resetRemainingLabel(0,now),resetRemainingLabel(at(-1),now))}),['1min后','59min后','1h后','5h后','1d后','7d后','未知','已到期']);
  assert.deepEqual(await page.evaluate(()=>[100,0,130,-10,null].map(used_percent=>{const node=renderQuotaWindow({seconds:18000,used_percent,reset_at:0});return [node.querySelector('.quota-percent').textContent,node.querySelector('.quota-fill')?.style.width||null,node.querySelector('.quota-reset').textContent]})),[['0%','0%','未知'],['100%','100%','未知'],['0%','0%','未知'],['100%','100%','未知'],['额度未知',null,'未知']]);
  assert.deepEqual(await page.locator('.concurrency-limit').evaluateAll(inputs=>inputs.map(x=>Number(x.value))),[3,3]);
  await row('account-a').fill('5');await saved();assert.equal(settings.active_overrides['account-a'],5);
  assert.match(await row('account-a').locator('..').locator('.slot-count').innerText(),/0 \/ 5/);
  assert.equal(await row('account-a').locator('..').locator('.slot').count(),5);
  await open();assert.equal(await row('account-a').inputValue(),'5');

  // An unrelated, invalid autoreset form cannot block concurrency-only saves.
  await page.locator('#auto-reset').check();await row('account-a').fill('4');await saved();
  assert.equal(settings.auto_reset.enabled,false);assert.equal(await page.locator('#auto-reset').isChecked(),true);
  await page.locator('#auto-reset').uncheck();
  assert.ok(writes.filter(x=>x.active_overrides).every(x=>Object.keys(x).length===1));

  await page.locator('.concurrency-select').first().check();await page.locator('#batch-limit').fill('6');
  await page.locator('#set-selected-limit').click();await saved();await page.waitForFunction(()=>document.querySelector('#set-selected-limit').dataset.feedbackState==='success');
  assert.deepEqual(ids.map(id=>settings.active_overrides[id]||3),[6,3]);
  await page.locator('#batch-limit').fill('7');await page.locator('#set-all-limit').click();await saved();await page.waitForFunction(()=>document.querySelector('#set-all-limit').dataset.feedbackState==='success');
  assert.deepEqual(ids.map(id=>settings.active_overrides[id]),[7,7]);
  await page.locator('#refresh').click();await page.waitForFunction(()=>document.querySelector('#refresh').dataset.feedbackState==='success');assert.equal(await row('account-b').inputValue(),'7');

  await page.locator('#floor-5h').fill('0');await page.locator('#floor-7d').fill('6.5');await page.locator('#ping-enabled').check();await page.locator('#ping-mode').selectOption('fixed_times');await page.locator('#ping-times').fill('12:00,17:00');
  const pingA=page.locator('.ping-account-row').first();await pingA.locator('.ping-override-enabled').check();await pingA.locator('.ping-credential-mode').selectOption('interval_5h');await pingA.locator('.ping-credential-enabled').check();await page.locator('#ping-save').click();await page.waitForFunction(()=>document.querySelector('#ping-feedback').textContent.includes('已保存'));await page.waitForFunction(()=>document.querySelector('#ping-save').dataset.feedbackState==='success');
  assert.equal(settings.floor_5h,0);assert.equal(settings.floor_7d,6.5);assert.equal(settings.auto_ping.enabled,true);assert.equal(settings.auto_ping.mode,'fixed_times');assert.equal(settings.auto_ping_overrides['account-a'].mode,'interval_5h');assert.equal(Object.hasOwn(settings.auto_ping_overrides,'account-b'),false);
  await open();assert.equal(await page.locator('#floor-5h').inputValue(),'0');assert.equal(await page.locator('.ping-account-row').first().locator('.ping-override-enabled').isChecked(),true);

  // Priority controls are durable checkboxes and the old editable endpoint is gone.
  const oneShot=page.locator('.priority-control').nth(0),continuous=page.locator('.priority-control').nth(1);
  await oneShot.check();await waitFor(()=>priorityState.one_shot_auth_id==='account-a');
  assert.equal(priorityControls.at(-1).mode,'one_shot');
  await continuous.check();await waitFor(()=>priorityState.continuous_auth_id==='account-a');
  assert.equal(priorityControls.at(-1).mode,'continuous');
  await oneShot.uncheck();await waitFor(()=>priorityState.one_shot_auth_id==='');
  assert.equal(await page.locator('.priority-control').count(),4);
  assert.equal(await page.evaluate(()=>fmt('0001-01-01T00:00:00Z')),'未知');
  assert.equal(await page.evaluate(()=>statusBadge({quantized_score:1,probe_error:'upstream_http_503',credential:{}}).textContent),'可路由');

  let before=writes.length;await row('account-a').fill('0');
  await page.waitForFunction(()=>document.querySelector('#concurrency-feedback').dataset.state==='error');
  assert.equal(writes.length,before);assert.equal(await row('account-a').inputValue(),'0');
  await row('account-a').fill('5');await row('account-a').press('Tab');await saved();
  assert.equal(writes.length,before+1);

  failNext=true;await row('account-a').fill('9');
  await page.waitForFunction(()=>document.querySelector('#concurrency-feedback').dataset.state==='error');
  assert.equal(settings.active_overrides['account-a'],5);assert.equal(await row('account-a').inputValue(),'9');
  assert.match(await row('account-a').locator('..').locator('.concurrency-note').innerText(),/未保存/);
  before=statusReads;await page.evaluate(()=>window.__automaticRefresh());assert.ok(statusReads>=before);assert.equal(await row('account-a').inputValue(),'9');
  await page.locator('#refresh').click();await saved();assert.equal(settings.active_overrides['account-a'],9);

  // An older in-flight write cannot overwrite a newer draft.
  holdNext=true;await row('account-a').fill('4');await waitFor(()=>release!==null);
  await row('account-a').fill('8');await row('account-a').press('Tab');
  const done=release;release=null;done();await saved();
  assert.equal(settings.active_overrides['account-a'],8);assert.equal(await row('account-a').inputValue(),'8');

  ignoreNext=true;await row('account-a').fill('6');
  await page.waitForFunction(()=>document.querySelector('#concurrency-feedback').dataset.state==='error');
  assert.equal(settings.active_overrides['account-a'],8);
  await page.locator('#refresh').click();await saved();assert.equal(settings.active_overrides['account-a'],6);
  await page.locator('#save').click();await waitFor(()=>writes.some(x=>Object.hasOwn(x,'auto_reset_enabled')));await page.waitForFunction(()=>document.querySelector('#save').dataset.feedbackState==='success');
  assert.ok(writes.every(x=>!Object.hasOwn(x,'max_active')));
  await open();assert.equal(await row('account-a').inputValue(),'6');assert.equal(await row('account-b').inputValue(),'7');
  if(process.argv[2])for(const theme of ['dark','light']){await page.evaluate(theme=>document.documentElement.dataset.theme=theme,theme);await page.evaluate(()=>window.scrollTo(0,0));await page.screenshot({path:process.argv[2]+'-top-'+theme+'.png'});await page.evaluate(()=>window.scrollTo(0,700));await page.screenshot({path:process.argv[2]+'-scroll-'+theme+'.png'});await page.evaluate(()=>window.scrollTo(0,0));await page.locator('section').last().screenshot({path:process.argv[2]+'-accounts-'+theme+'.png'})}
  await page.setViewportSize({width:760,height:1050});
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  await page.locator('.concurrency-limit').first().focus();await page.locator('.concurrency-limit').first().press('Tab');
  assert.ok(await page.evaluate(()=>document.activeElement.matches('input,button,select')));
  if(process.argv[2])await page.screenshot({path:process.argv[2]+'-narrow.png'});
  assert.deepEqual(errors,[]);
  console.log(JSON.stringify({result:'CONCURRENCY_UI_AUTOSAVE_REFRESH_FAILURE_RACE_OK',default:3,default_editor:false,
   single_autosave:true,selected_and_all_batch:true,refresh_persists:true,invalid_input_rejected:true,
   failed_write_retains_draft:true,newer_edit_wins:true,effective_gate_verified:true,default_never_submitted:true,
   other_form_preserved:true,email_only:true,internal_labels_hidden:true,green_priority_indicator:true,
   quota_progress_and_reset:true,narrow_layout_and_keyboard:true,themes:['dark','light']}));
 }finally{await browser.close()}
})().catch(error=>{console.error(error.stack);process.exit(1)});
