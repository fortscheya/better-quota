const buttonFeedbackTimers=new WeakMap();
function buttonFeedback(button,state,text,duration=1200){
 if(!button)return;
 const original=button.dataset.feedbackOriginal||button.textContent;button.dataset.feedbackOriginal=original;clearTimeout(buttonFeedbackTimers.get(button));button.textContent=text;button.dataset.feedbackState=state;button.setAttribute('aria-busy',state==='busy'?'true':'false');button.classList.remove('button-feedback-busy','button-feedback-success','button-feedback-error');button.classList.add('button-feedback-'+state);button.disabled=state==='busy';
 if(state!=='busy'&&duration>0)buttonFeedbackTimers.set(button,setTimeout(()=>{if(button.isConnected){button.textContent=original;button.dataset.feedbackState='';button.classList.remove('button-feedback-'+state)}},duration));
}
async function runButtonAction(button,action,labels={}){buttonFeedback(button,'busy',labels.busy||'处理中…',0);try{const result=await action();if(result===false){buttonFeedback(button,'error',labels.error||'未完成');return false}buttonFeedback(button,'success',labels.success||'已完成');return result}catch(error){buttonFeedback(button,'error',labels.error||'失败');showError(error);throw error}}
// Optional authenticated parent bridge. Standalone access uses the CPA
// management key entered in the page and never stores it outside memory.
const hostChannel='better-quota-host-v1',hostPending=new Map();let hostConnected=false;
function bridgeRequest(path,body,timeout=15000){
 return new Promise((resolve,reject)=>{
  const id=crypto.randomUUID(),alarm=setTimeout(()=>{hostPending.delete(id);reject(Error('管理面板连接超时，请刷新页面；写入操作请先核对状态，勿直接重复提交'))},timeout);
  hostPending.set(id,{resolve,reject,alarm});
  parent.postMessage({channel:hostChannel,kind:'request',id,path,method:body===undefined?'GET':'POST',...(body===undefined?{}:{body})},location.origin);
 });
}
addEventListener('message',event=>{
 if(event.source!==parent||event.origin!==location.origin||event.data?.channel!==hostChannel||event.data?.kind!=='response')return;
 const m=event.data,p=hostPending.get(m.id);if(!p)return;
 clearTimeout(p.alarm);hostPending.delete(m.id);
 if(m.ok)p.resolve(m.data);else{const error=Error(m.data?.error||(m.status===401?'管理面板登录已失效，请重新登录管理面板':'管理面板请求失败（'+m.status+'）'));Object.assign(error,m.data||{},{status:m.status});p.reject(error)}
});
async function connectHost(){
 if(parent===window)return;
 try{await bridgeRequest('/hello',undefined,3000);hostConnected=true;el('login').hidden=true;await load();startRefresh();}
 catch(e){if(hostConnected){showError(e);return}el('error').textContent='当前宿主未提供免登录连接，请输入管理密钥。'}
}

let automationDirty=false,automationSection=null;
const pingOverrideDrafts=new Map();
function automationTimes(raw){return String(raw||'').split(',').map(x=>x.trim()).filter(Boolean)}
function ensureAutomationSettings(){
 if(automationSection)return automationSection;
 automationSection=document.createElement('section');automationSection.id='automation-settings';
 automationSection.innerHTML='<div class="bar spread"><div><h2>预热与额度底线</h2><p>预热会发送极小的真实 Codex 请求并消耗少量额度；全局计划可被单个凭证覆盖。时间均为 UTC+8。</p></div><div class="bar"><span class="hint">保存到插件 state，重启后继续生效</span><button id="ping-save">保存预热与底线</button></div></div><div class="ping-grid"><div><label><input id="ping-enabled" type="checkbox">启用全局 auto-ping</label><p class="hint">默认关闭；凭证可以单独启用或关闭。</p></div><div><label for="ping-mode">全局发送方式</label><select id="ping-mode"><option value="interval_5h">每隔 5 小时</option><option value="daily_5h">每日 00:00 起按 5 小时时间格</option><option value="fixed_times">每天指定时间点</option></select></div><div><label for="ping-times">全局时间点（UTC+8）</label><input id="ping-times" class="ping-times" type="text" placeholder="例如 12:00,17:00"><p class="hint">仅指定时间点模式使用；多个时间用逗号分隔。</p></div></div><div class="ping-grid"><div><label for="floor-5h">5h 限额下限（剩余 %）</label><input id="floor-5h" class="floor-input" type="number" min="0" max="100" step="0.1"></div><div><label for="floor-7d">周限额下限（剩余 %）</label><input id="floor-7d" class="floor-input" type="number" min="0" max="100" step="0.1"></div><div><label for="ping-model">预热模型</label><input id="ping-model" type="text" readonly><p class="hint">模型沿用插件配置；如需更换请修改配置文件。</p></div></div><h3>按凭证设置 auto-ping</h3><p class="hint">关闭“继承全局”后，该凭证可以独立选择启用/关闭、每隔 5 小时或指定时间点。</p><div id="ping-accounts" class="ping-account-list"></div><p id="ping-feedback" class="hint ping-feedback" role="status" aria-live="polite"></p>';
 const anchor=document.querySelector('#autoreset-settings');if(anchor)anchor.after(automationSection);else document.querySelector('#main')?.prepend(automationSection);
 for(const id of ['ping-enabled','ping-mode','ping-times','floor-5h','floor-7d'])document.getElementById(id).addEventListener('input',()=>{automationDirty=true;document.getElementById('ping-feedback').textContent='有未保存修改。'});
 document.getElementById('ping-save').onclick=()=>runButtonAction(document.getElementById('ping-save'),saveAutomationSettings,{busy:'保存中…',success:'已保存',error:'保存失败'}).catch(()=>{});
 return automationSection;
}
function pingOverrideRow(id,credential,override,global){
 const row=document.createElement('div');row.className='ping-account-row ping-account-grid';row.dataset.account=id;
 const name=document.createElement('div'),title=document.createElement('strong');title.textContent=accountEmail(credential);name.append(title);row.append(name);
 const inherit=document.createElement('label'),check=document.createElement('input');check.type='checkbox';check.className='ping-override-enabled';check.checked=!!override;inherit.append(check,document.createTextNode('单独设置'));row.append(inherit);
 const fields=document.createElement('div');fields.className='ping-fields';const enabled=document.createElement('label'),enabledCheck=document.createElement('input');enabledCheck.type='checkbox';enabledCheck.className='ping-credential-enabled';enabledCheck.checked=override?!!override.enabled:!!global.enabled;enabled.append(enabledCheck,document.createTextNode('启用'));fields.append(enabled);
 const mode=document.createElement('select');mode.className='ping-credential-mode';for(const [value,label] of [['interval_5h','每隔 5 小时'],['daily_5h','每日 5 小时格'],['fixed_times','指定时间点']]){const option=document.createElement('option');option.value=value;option.textContent=label;mode.append(option)}mode.value=override?.mode||global.mode;fields.append(mode);row.append(fields);
 const times=document.createElement('input');times.className='ping-credential-times';times.type='text';times.placeholder='12:00,17:00';times.value=(override?.times||global.times||[]).join(',');row.append(times);
 const toggle=()=>{fields.hidden=!check.checked;times.hidden=!check.checked};toggle();for(const input of [check,enabledCheck,mode,times])input.addEventListener('input',()=>{automationDirty=true;toggle();document.getElementById('ping-feedback').textContent='有未保存修改。'});
 return row;
}
function renderAutomationSettings(d){
 const section=ensureAutomationSettings();if(automationDirty)return;
 const global=d.config.auto_ping||{};document.getElementById('ping-enabled').checked=!!global.enabled;document.getElementById('ping-mode').value=global.mode||'daily_5h';document.getElementById('ping-times').value=(global.times||[]).join(',');document.getElementById('floor-5h').value=global.floor_5h??d.config.floor_5h??'';document.getElementById('floor-7d').value=global.floor_7d??d.config.floor_7d??'';document.getElementById('ping-model').value=global.model||'';
 const list=document.getElementById('ping-accounts'),overrides=d.auto_ping_overrides||{};list.replaceChildren();for(const a of d.dashboard_accounts||[]){const id=a.credential.id;list.append(pingOverrideRow(id,a.credential,Object.hasOwn(overrides,id)?overrides[id]:null,{enabled:!!global.enabled,mode:global.mode||'daily_5h',times:global.times||[]}))}section.hidden=false;
}
async function saveAutomationSettings(){
 const feedback=document.getElementById('ping-feedback'),enabled=document.getElementById('ping-enabled').checked,mode=document.getElementById('ping-mode').value,times=automationTimes(document.getElementById('ping-times').value),model=document.getElementById('ping-model').value.trim(),floor5=Number(document.getElementById('floor-5h').value),floor7=Number(document.getElementById('floor-7d').value);
 const validTimes=times.every(x=>/^([01]\d|2[0-3]):[0-5]\d$/.test(x));if(!Number.isFinite(floor5)||floor5<0||floor5>100||!Number.isFinite(floor7)||floor7<0||floor7>100||!validTimes||enabled&&(model===''||(mode==='fixed_times'&&!times.length))){feedback.textContent='请检查底线范围、HH:MM 时间点和启用条件。';return false}
 const overrides={};for(const row of document.querySelectorAll('.ping-account-row')){const id=row.dataset.account;if(!row.querySelector('.ping-override-enabled').checked){overrides[id]=null;continue}const item={enabled:row.querySelector('.ping-credential-enabled').checked,mode:row.querySelector('.ping-credential-mode').value,times:automationTimes(row.querySelector('.ping-credential-times').value)};if(item.enabled&&item.mode==='fixed_times'&&!item.times.length){feedback.textContent='指定时间点模式至少需要一个时间。';return false}if(!item.times.every(x=>/^([01]\d|2[0-3]):[0-5]\d$/.test(x))){feedback.textContent='凭证时间点必须使用 HH:MM 格式。';return false}overrides[id]=item}
 try{feedback.textContent='正在保存…';await api('/settings',{floor_5h:floor5,floor_7d:floor7,auto_ping_enabled:enabled,auto_ping_mode:mode,auto_ping_times:times,auto_ping_overrides:overrides});automationDirty=false;await load();feedback.textContent='预热与额度底线已保存并生效。';return true}catch(e){feedback.textContent='保存失败，修改尚未确认生效。';showError(e);throw e}
}
