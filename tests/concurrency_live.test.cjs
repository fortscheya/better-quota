// Uses a fresh browser and changes one concurrency limit temporarily, then
// restores its original override. No autoreset, priority, proxy, or auth edits.
const fs=require('node:fs'),path=require('node:path'),http=require('node:http'),assert=require('node:assert/strict');
const {chromium}=require('playwright');
const cpa=path.resolve(__dirname,'../../codex_CPA_SDK/cpa');
const managerKey=fs.readFileSync(path.join(cpa,'secrets/cpamp-admin-key'),'utf8').trim();
const managementKey=fs.readFileSync(path.join(cpa,'secrets/management-key'),'utf8').trim();
function request(endpoint,body){return new Promise((resolve,reject)=>{
 const data=body===undefined?undefined:JSON.stringify(body);
 const req=http.request('http://127.0.0.1:8317/v0/management'+endpoint,{method:data===undefined?'GET':'POST',headers:{Authorization:'Bearer '+managementKey,'Content-Type':'application/json'}},response=>{
  let raw='';response.setEncoding('utf8');response.on('data',chunk=>raw+=chunk);response.on('end',()=>{try{if(response.statusCode!==200)throw Error('CPA request status '+response.statusCode);resolve(JSON.parse(raw))}catch(error){reject(error)}});
 });req.on('error',reject);if(data)req.write(data);req.end();
})}
(async()=>{
 const before=await request('/plugins/better-quota/status'),original=before.config.active_overrides||{};
 const browser=await chromium.launch({headless:true,channel:'chrome'});let actor=null,expectedOverride=null;
 const results={};
 try{
  const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.name));
  await page.goto('http://127.0.0.1:18317/management.html#/plugin-pages/better-quota/0');
  await page.locator('input[type=password]').fill(managerKey);await page.getByRole('button',{name:'登录',exact:true}).click();
  await page.locator('input[type=password]').waitFor({state:'hidden'});
  await page.goto('http://127.0.0.1:18317/management.html#/plugin-pages/better-quota/0');
  await page.frameLocator('iframe').locator('#main').waitFor({state:'visible',timeout:20000});
  const frame=page.frames().find(f=>f.url().endsWith('/plugins/better-quota/dashboard'));
  const input=frame.locator('.concurrency-limit').first();actor=await input.getAttribute('data-account');
  assert.equal(await frame.locator('#max-active').count(),0);assert.equal(await frame.locator('#key').inputValue(),'');
  assert.equal(await frame.locator('#connection').innerText(),'宿主会话');
  assert.equal(before.config.max_active,3);assert.ok(before.dashboard_accounts.every(a=>a.concurrency_limit===(original[a.credential.id]||3)));
  const target=(original[actor]||3)===4?5:4;
  await input.fill(String(target));expectedOverride=target;
  await frame.waitForFunction(()=>document.querySelector('#concurrency-feedback').dataset.state==='saved');
  let status=await request('/plugins/better-quota/status');assert.equal(status.dashboard_accounts.find(a=>a.credential.id===actor).concurrency_limit,target);
  const state=JSON.parse(fs.readFileSync(path.join(cpa,'plugins/data/better-quota-v0.1.3/state.json'),'utf8'));
  assert.equal(state.settings.active_overrides[actor],target);results.disk_persisted=true;
  await frame.goto(frame.url());await frame.locator('#main').waitFor({state:'visible'});
  assert.equal(await frame.locator('.concurrency-limit').first().inputValue(),String(target));
  assert.match(await frame.locator('.concurrency-cell').first().locator('.slot-count').innerText(),new RegExp(' / '+target+' '));
  results.plugin_refresh_persists=true;results.slots_match_effective_limit=true;
  await frame.locator('.concurrency-limit').first().fill(String(original[actor]||3));expectedOverride=original[actor]||3;
  await frame.waitForFunction(()=>document.querySelector('#concurrency-feedback').dataset.state==='saved');
  assert.deepEqual(errors,[]);results.embedded_passwordless=true;results.default_editor_absent=true;
 }finally{
  await browser.close();
  if(actor&&expectedOverride!==null){
   const current=await request('/plugins/better-quota/status');
   assert.equal(current.config.active_overrides?.[actor],expectedOverride,'Concurrent edit detected; refusing to overwrite it');
   await request('/plugins/better-quota/settings',{active_overrides:{[actor]:Object.hasOwn(original,actor)?original[actor]:null}});
  }
 }
 const final=await request('/plugins/better-quota/status');
 assert.equal(final.config.max_active,3);assert.deepEqual(final.config.active_overrides||{},original);
 const comparable=value=>{value=structuredClone(value);delete value.active_overrides;return value};
 assert.deepEqual(comparable(final.config),comparable(before.config));
 results.version=final.version;results.default=final.config.max_active;results.account_count=final.dashboard_accounts.length;
 results.final_limits=final.dashboard_accounts.map(a=>a.concurrency_limit);results.original_overrides_restored=true;results.other_settings_preserved=true;
 if(process.argv[2])fs.writeFileSync(process.argv[2],JSON.stringify(results,null,2)+'\n',{mode:0o600});
 console.log(JSON.stringify({result:'CONCURRENCY_LIVE_PERSISTENCE_OK',...results}));
})().catch(error=>{console.error(JSON.stringify({error:error.name,message:error.message.split('\n')[0].slice(0,160)}));process.exit(1)});
